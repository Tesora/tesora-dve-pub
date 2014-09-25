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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import com.tesora.dve.common.PECollectionUtils;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.PELockedException;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupTopicPublisher;
import com.tesora.dve.groupmanager.PurgeWorkerGroupCaches;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.PEPersistentGroup.TStorageGroup;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerManager;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;


/**
 * A <b>StorageGroup</b> is a collection of instances of {@link PersistentSite}
 * used to represent the sites which hold a set of data relative to a 
 * {@link DistributionModel}.
 * <p/>
 * The <b>StorageGroup</b> may be persisted in the catalog,
 * or it may be a non-persistent <b>StorageGroup</b>, for example when
 * selecting a subset of a <b>StorageGroup</b> to map data to as an 
 * intermediate step in a {@link QueryPlan}.
 *
 */
@Entity
@Table(name="persistent_group")
public class PersistentGroup implements CatalogEntity, StorageGroup {

	private static final long serialVersionUID = 1L;
	public static final String TEMP_NAME = "TempGroup";
	public static final String PERSISTENT_GROUP_CS_HEADER_VAL = "Persistent Group";

	public static final String PERSISTENT_GROUP_SUPPRESS_CACHE_PURGE = "PersistentGroup.suppressCachePurge";
	@Id
	@GeneratedValue
	@Column( name="persistent_group_id" )
	int id;
		
	@Column(name="name", unique=true, nullable=false)
	String name;
	
	@OneToMany(mappedBy="storageGroup", cascade=CascadeType.ALL, fetch=FetchType.EAGER)
	@OrderBy("version ASC")
	List<StorageGroupGeneration> generations;
	
	public PersistentGroup() {
	}
	
	/**
	 * Constructor to create a <b>StorageGroup</b> with the given <em>name</em>,
	 * typically a persistent <b>StorageGroup</b>.
	 * 
	 * @param name name of the <b>StorageGroup</b>
	 * @throws PEException 
	 */
	public PersistentGroup(String name) {
		this.name = name;
		generations = new ArrayList<StorageGroupGeneration>();
		generations.add(new StorageGroupGeneration(this, /* ver */ 0));
	}
	
	/**
	 * Constructor to create a <b>StorageGroup</b> containing the provided 
	 * {@link PersistentSite}.  This would be a non-persistent <b>StorageGroup</b>.
	 * 
	 * @param site the {@link PersistentSite} in the group
	 * @throws PELockedException 
	 */
	public PersistentGroup(PersistentSite site) throws PELockedException {
		this(TEMP_NAME);
		generations.get(0).addStorageSite(site);
	}

	public PersistentGroup(Collection<PersistentSite> newSites) {
		this(TEMP_NAME);
		try {
			generations.get(0).add(newSites);
		} catch (PELockedException e) {
			throw new PECodingException("Temp groups should never be locked", e);
		}
	}

	/**
	 * Locks the <b>StorageGroup</b>.  This means that the list of 
	 * {@link PersistentSite} in the group may not be modifed, and is
	 * used when data is added to a table using a {@link DistributionModel}
	 * which will not be able to find records if the list of sites in the
	 * <b>StorageGroup</b> is changed (for example, {@link RandomDistributionModel}
	 * and {@link BroadcastDistributionModel} are tolerant of changes,
	 * but DynamicDistributionModel and RangeDistributionModel
	 * are not). 
	 */
	public void lockGroup() {
		StorageGroupGeneration lastGen = getLastGen();
		if (!lastGen.isLocked())
			lastGen.lock();
	}
	
	public StorageGroupGeneration getLastGen() {
		return generations.get(generations.size()-1);
	}
	
	public final List<PersistentSite> getStorageSites() {
		List<PersistentSite> allSites = new ArrayList<PersistentSite>(generations.get(0).getStorageSites());
		if (generations.size() > 1) {
			Set<PersistentSite> siteSet = new HashSet<PersistentSite>(allSites);
			for (int i = 1; i < generations.size(); ++i) {
				for (PersistentSite site : generations.get(i).getStorageSites()) {
					if (!siteSet.contains(site)) {
						siteSet.add(site);
						allSites.add(site);
					}
				}
			}
		}
		return allSites;
	}
	
	public PersistentGroup anySite() throws PELockedException {
		final List<PersistentSite> allSites = getStorageSites();
		return new PersistentGroup(PECollectionUtils.selectRandom(allSites));
	}
	
	/**
	 * @return
	 * @throws PELockedException
	 */
	public PersistentSite anySiteInLatestGeneration() throws PELockedException {
		final List<PersistentSite> latestSites = getLastGen().getStorageSites();
		return PECollectionUtils.selectRandom(latestSites);
	}

	/**
	 * Returns the name of the <b>StorageGroup</b>.
	 * 
	 * @return <b>StorageGroup</b> name
	 */
	@Override
	public String getName() {
		return this.name;
	}

	public Integer getLastGeneration() {
		if (generations.isEmpty())
			return null;
		return generations.get(generations.size() - 1).id;
	}
	
	public final List<StorageGroupGeneration> getGenerations() {
		return generations;
	}

	@Override
	public String toString() {
		return PEStringUtils.toString(getName(), generations);
	}
	
	public static int computeHashCode(StorageGroup sg) {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sg.getName() == null) ? 0 : sg.getName().hashCode());
		return result;
	}
	
	@Override
	public int hashCode() {
		return computeHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return computeEquals(this,obj);
	}

	public static boolean computeEquals(StorageGroup left, Object right) {
		if (left == right) return true; // NOPMD by doug on 15/01/13 3:56 PM
		if (right == null) return false;

		if (!(right instanceof PersistentGroup || right instanceof TStorageGroup)) return false;
		StorageGroup other = (StorageGroup) right;
		if (left.getName() == null) {
			if (other.getName() != null)
				return false;
		} else if (!left.getName().equals(other.getName())) {
			return false;			
		}
		return true;
	}
	
	@Override
	public int getId() {
		return id;
	}

	public void addStorageSite(PersistentSite site) throws PEException {
		if (getLastGen().isLocked())
			throw new PEException("Cannot add site to locked persistent group");
		getLastGen().addStorageSite(site);
	}

	public void addGeneration(SSConnection ssCon, WorkerGroup wg, StorageGroupGeneration newGen) throws Throwable {
		if (false == this.equals(wg.getGroup()))
			throw new PEException("WorkerGroup does not match StorageGroup");
		if (generations.size() > 0)
			lockGroup();
		List<UserTable> tables = ssCon.getCatalogDAO().findAllTablesInPersistentGroup(this);
		ListSet<UserDatabase> dbs = new ListSet<UserDatabase>();
		for(UserTable ut : tables)
			dbs.add(ut.getDatabase());
		for(UserDatabase udb : dbs)
			wg.assureDatabase(ssCon, udb);
		for (UserTable t : tables) {
			t.prepareGenerationAddition(ssCon, wg, newGen);
		}
		generations.add(newGen);
		wg.markForPurge();
		onUpdate();
		WorkerGroupFactory.clearGroupFromCache(ssCon, this);
		ssCon.clearWorkerGroupCache(this);
	}

	public void addAllSites(Collection<PersistentSite> sites) throws PELockedException {
		getLastGen().add(sites);
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		ColumnSet showColumnSet = new ColumnSet();
		showColumnSet.addColumn(PERSISTENT_GROUP_CS_HEADER_VAL, 255, "varchar", Types.VARCHAR);
		if (!cqo.isPlural())
			showColumnSet.addColumn("Latest Generation",3,"integer",Types.INTEGER);
		return showColumnSet;
	}
	
	@Override
	public boolean isTemporaryGroup() {
		return TEMP_NAME.equals(name);
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
		if (!cqo.isPlural()) {
			if (generations.isEmpty()) 
				rr.addResultColumn(null,true);
			else
				rr.addResultColumn(generations.get(generations.size() - 1).id, false);
		}
		
		return rr;
	}

	@Override
	public void removeFromParent() {
		// do nothing
	}
	
	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		ArrayList<CatalogEntity> out = new ArrayList<CatalogEntity>();
		out.addAll(generations);
		for(StorageGroupGeneration sgg : generations) {
			out.addAll(sgg.getDependentEntities(c));
		}
		// also any ranges
		out.addAll(c.findRangesOnGroup(getName()));
		return out;
	}

	@Override
	public void returnWorkerSites(WorkerManager workerManager,
			Collection<? extends StorageSite> sites) throws PEException {
		// Nothing to do here as sites are statically allocated to persistent groups
	}

	@Override
	public void provisionGetWorkerRequest(GetWorkerRequest getWorkerRequest) throws PEException {
		getWorkerRequest.fulfillGetWorkerRequest(getStorageSites());
	}

	@Override
	public int sizeForProvisioning() throws PEException {
		return getStorageSites().size();
	}

	@Override
	public void onUpdate() {
		onDrop();
	}

	@Override
	public void onDrop() {
		if (!Boolean.getBoolean(PERSISTENT_GROUP_SUPPRESS_CACHE_PURGE)) {
			PurgeWorkerGroupCaches purgeMessage = new PurgeWorkerGroupCaches(this);
            Singletons.require(GroupTopicPublisher.class).publish(purgeMessage);
		}
	}
 }
