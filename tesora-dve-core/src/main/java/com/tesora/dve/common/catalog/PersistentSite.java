package com.tesora.dve.common.catalog;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.tesora.dve.worker.WorkerFactory;
import io.netty.channel.EventLoopGroup;
import org.apache.log4j.Logger;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupTopicPublisher;
import com.tesora.dve.groupmanager.SiteFailureMessage;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.statistics.SiteStatKey.SiteType;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;
import com.tesora.dve.sql.schema.PEStorageSite.TCacheSite;
import com.tesora.dve.worker.AdditionalConnectionInfo;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

@InfoSchemaTable(logicalName = "storage_site", views = {
		@TableView(view = InfoView.SHOW, name = "persistent site", pluralName = "persistent sites", columnOrder = {
				ShowSchema.PersistentSite.NAME, ShowSchema.PersistentSite.HA_TYPE, ShowSchema.PersistentSite.URL }, extension = true, priviledged = true),
		@TableView(view = InfoView.INFORMATION, name = "storage_site", pluralName = "", columnOrder = { "name",
				"haType", "masterUrl" }, extension = true, priviledged = true) })
@Entity
@Table(name = "storage_site", uniqueConstraints = @UniqueConstraint(columnNames = "name"))
public class PersistentSite implements CatalogEntity, StorageSite {

	transient private Logger logger = Logger.getLogger(PersistentSite.class);

	private static final long serialVersionUID = 1L;

    @Id
	@GeneratedValue
	int id;

	String name;

	String haType;

	@OneToMany(cascade = CascadeType.ALL, mappedBy = "storageSite", fetch = FetchType.EAGER)
	@OrderBy("id ASC")
	List<SiteInstance> siteInstances;

	transient SiteInstance masterInstance = null;

	private transient ColumnSet showColumnSet = null;

	PersistentSite() {
	}

	public PersistentSite(String name, SiteInstance siteInstance) throws PEException {
		this(name);
		addInstance(siteInstance);
		haType = WorkerFactory.SINGLE_DIRECT_HA_TYPE;
	}

	public PersistentSite(String name) {
		this(name, WorkerFactory.SINGLE_DIRECT_HA_TYPE);
	}

	public PersistentSite(String name, String haType) {
		this.name = name;
		this.haType = haType;
		this.siteInstances = new ArrayList<SiteInstance>();
	}

	@InfoSchemaColumn(logicalName = "masterUrl", fieldName = "", // "masterSite",
	sqlType = java.sql.Types.VARCHAR, sqlWidth = 255, views = {
			@ColumnView(view = InfoView.SHOW, name = ShowSchema.PersistentSite.URL),
			@ColumnView(view = InfoView.INFORMATION, name = "masterUrl") })
	@Override
	public String getMasterUrl() {
		if (getMasterInstance() == null) {
			return null;
		}
		return getMasterInstance().getInstanceURL();
	}

	@InfoSchemaColumn(logicalName = "name", fieldName = "name", sqlType = java.sql.Types.VARCHAR, sqlWidth = 255, views = {
			@ColumnView(view = InfoView.SHOW, name = ShowSchema.PersistentSite.NAME, orderBy = true, ident = true),
			@ColumnView(view = InfoView.INFORMATION, name = "name", orderBy = true, ident = true) })
	@Override
	public String getName() {
		return name;
	}

	@InfoSchemaColumn(logicalName = "haType", fieldName = "haType", sqlType = java.sql.Types.VARCHAR, sqlWidth = 25, views = {
			@ColumnView(view = InfoView.SHOW, name = ShowSchema.PersistentSite.HA_TYPE),
			@ColumnView(view = InfoView.INFORMATION, name = "haType") })
	public String getHAType() {
		return haType;
	}

	@Override
	@InfoSchemaColumn(logicalName = "id", fieldName = "id", sqlType = java.sql.Types.INTEGER, views = {})
	public int getId() {
		return id;
	}

	public void addInstance(SiteInstance instance) throws PEException {
		if (instance == null)
			return;
		getSiteInstances().add(instance);
		instance.setStorageSite(this);
		if (getMasterInstance() == null)
			setMasterInstance(instance);
	}

	public void removeInstance(SiteInstance instance) {
		synchronized (this) {
			if (instance.equals(getMasterInstance()))
				masterInstance = null;
			for (Iterator<SiteInstance> i = getSiteInstances().iterator(); i.hasNext();) {
				if (instance.equals(i.next())) {
					i.remove();
					break;
				}
			}
			if (masterInstance == null && !getSiteInstances().isEmpty())
				setMasterInstance(siteInstances.get(0));
		}
	}

	public void addAll(SiteInstance[] replicants) throws PEException {
		for (SiteInstance instance : replicants)
			addInstance(instance);
	}

	public void setMasterInstance(SiteInstance instance) {
		synchronized (this) {
			if (getMasterInstance() != null)
				masterInstance.setMaster(false);
			masterInstance = instance;
			if (masterInstance != null)
				masterInstance.setMaster(true);
		}
	}

	public SiteInstance getMasterInstance() {
		synchronized (this) {
			if (masterInstance == null) {
				for (SiteInstance site : getSiteInstances()) {
					if (site.isMaster() && site.isEnabled()) {
						if (masterInstance == null)
							masterInstance = site;
						else
							throw new PECodingException("Site " + getName()
									+ " has multiple master instances defined - aborting master instance selection");
					}
				}
			}
		}
		return masterInstance;
	}

	@Override
	public PersistentSite getRecoverableSite(CatalogDAO c) {
		return this;
	}

	@Override
	public Worker pickWorker(Map<StorageSite, Worker> workerMap) throws PEException {
		return workerMap.get(this);
	}

	@Override
	public void annotateStatistics(LogSiteStatisticRequest sNotice) {
		sNotice.setSiteDetails(getName(), SiteType.PERSISTENT);
	}

	@Override
	public void incrementUsageCount() {
	}

	@Override
	public Worker createWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, EventLoopGroup preferredEventLoop) throws PEException {
		// If the user is the admin user then use site creds instead of user creds
		if(auth.isAdminUser())
			return getWorkerFactory().newWorker(getMasterInstance().getAuthentication(), additionalConnInfo, this, preferredEventLoop);
		
		return getWorkerFactory().newWorker(auth, additionalConnInfo, this, preferredEventLoop);
	}

    protected WorkerFactory getWorkerFactory() {
		return WorkerFactory.getWorkerFactory(getHAType());
	}

	public static void addWorkerFactory(String haType, WorkerFactory factory) {
        WorkerFactory.registerFactory(haType, factory);
    }

    public static boolean isValidHAType(String haType) {
		return WorkerFactory.hasFactoryFor(haType);
	}

    @Override
	public String toString() {
		return getClass().getSimpleName() + "(" + name + "/" + id + "," + getMasterInstance() + ")";
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		if (showColumnSet == null) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Persistent Site", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("URL", 255, "varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
		rr.addResultColumn(this.masterInstance, false);
		return rr;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// TODO Actually implement the removal of this instance from the parent
	}

	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// TODO Return a valid list of dependents
		return Collections.emptyList();
	}

	public static int computeHashCode(StorageSite ss) {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ss.getName() == null) ? 0 : ss.getName().hashCode());
		return result;

	}

	@Override
	public int hashCode() {
		return computeHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		/*
		 * if (this == obj) return true; if (obj == null) return false; if
		 * (getClass() != obj.getClass()) return false; PersistentSite other =
		 * (PersistentSite) obj; if (name == null) { if (other.name != null)
		 * return false; } else if (!name.equals(other.name)) return false;
		 * return true;
		 */
		return computeEquals(this, obj);
	}

	public static boolean computeEquals(StorageSite left, Object right) {
		if (left == right)
			return true; // NOPMD by doug on 15/01/13 3:58 PM
		if (right == null)
			return false;
		if (!(left instanceof PersistentSite || left instanceof TCacheSite))
			return false;
		if (!(right instanceof PersistentSite || right instanceof TCacheSite))
			return false;
		StorageSite other = (StorageSite) right;
		if (left.getName() == null) {
			if (other.getName() != null)
				return false;
		} else if (!left.getName().equals(other.getName()))
			return false;
		return true;
	}

	@Override
	public void onSiteFailure(CatalogDAO c) throws PEException {
		logger.warn("Failure on site " + this + " (instance " + getMasterInstance().getName() + ", url: "
				+ getMasterUrl() + ")");
		getWorkerFactory().onSiteFailure(this);
	}

	public void ejectMaster() throws PEException {
		String failedMasterURL = getMasterUrl();
		CatalogDAO c = CatalogDAOFactory.newInstance();
		c.begin();
		try {
			PersistentSite theSite = c.findByKey(PersistentSite.class, this.getId());
			c.refreshForLock(theSite);
			// If the master after refresh doesn't match the master that we had
			// when we came in,
			// the master site has already been switched
			if (failedMasterURL.equals(theSite.getMasterUrl())) {
				SiteInstance newMaster = null;
				for (SiteInstance instance : theSite.getSiteInstances()) {
					if (!instance.isMaster() && instance.isEnabled()) {
						newMaster = instance;
						break;
					}
				}
				theSite.getMasterInstance().failSiteInstance();
				theSite.setMasterInstance(newMaster);
				c.commit();
				if (newMaster != null) {
					logger.info("Site " + theSite + " fails over to " + theSite.getMasterInstance().getName()
							+ " (url: " + theSite.getMasterUrl() + ")");
					SiteFailureMessage sfm = new SiteFailureMessage(theSite.getId());
                    Singletons.require(GroupTopicPublisher.class).publish(sfm);
				} else
					throw new PEException("No database instances available for site failover for site " + theSite);
			}
		} catch (PEException e) {
			c.rollback(e);
			throw e;
			// } finally {
			// c.close();
		}
	}

	@Override
	public String getInstanceIdentifier() {
		return getWorkerFactory().getInstanceIdentifier(this, getMasterInstance());
	}

	public void setHaType(String haType) {
		this.haType = haType;
	}

	public final List<SiteInstance> getSiteInstances() {
		return siteInstances;
	}

	@Override
	public int getMasterInstanceId() {
		return getMasterInstance().getId();
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

	@Override
	public boolean supportsTransactions() {
		return true;
	}

	@Override
	public boolean hasDatabase(UserVisibleDatabase ctxDB) {
		// always true
		return true;
	}

	@Override
	public void setHasDatabase(UserVisibleDatabase ctxDB) throws PEException {
		throw new PECodingException("Invalid call to StorageSite.setHasDatabase");
	}
}
