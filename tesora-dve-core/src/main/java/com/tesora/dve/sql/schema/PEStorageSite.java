package com.tesora.dve.sql.schema;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.server.statistics.SiteStatKey.SiteType;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaCollectionCacheKey;
import com.tesora.dve.sql.util.Accessor;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.worker.SingleDirectWorker;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public class PEStorageSite extends Persistable<PEStorageSite, PersistentSite> {

	public static final String DIFF_TAG = "Persistent site";
	
	String jdbcURL = null;
	String jdbcUser = null;
	String jdbcPassword = null;
	String haType = null;
	PESiteInstance master = null;
	List<PESiteInstance> siteInstancesToAdd = null;
	List<PESiteInstance> siteInstancesToDrop = null;
	
	public PEStorageSite(SchemaContext pc, Name n, String url, String user, String password) {
		super(getSiteKey(n));

		setName(n);

		haType = SingleDirectWorker.HA_TYPE;
		jdbcURL = url;
		jdbcUser = user;
		jdbcPassword = password;

		setPersistent(pc,null,null);
	}
	
	public PEStorageSite(SchemaContext pc, Name n, String haType, PESiteInstance master, List<PESiteInstance> siteInstances) {
		super(getSiteKey(n));

		setName(n);

		this.haType = haType;
		this.master = master;
		this.siteInstancesToAdd = siteInstances;

		setPersistent(pc,null,null);
	}
	
	private PEStorageSite(PersistentSite ss, SchemaContext pc) {
		super(getSiteKey(ss.getName()));

		pc.startLoading(this,ss);

		setName(new UnqualifiedName(ss.getName()));
		jdbcURL = ss.getMasterUrl();
		if (ss.getMasterInstance() != null) {
			jdbcUser = ss.getMasterInstance().getUser();
			jdbcPassword = ss.getMasterInstance().getDecryptedPassword();
		}

		setPersistent(pc,ss,ss.getId());
		this.haType = ss.getHAType();
		if (ss.getMasterInstance() != null)
			master = PESiteInstance.load(ss.getMasterInstance(), pc);
		pc.finishedLoading(this, ss);
	}
	
	public static PEStorageSite load(PersistentSite ss, SchemaContext pc) {
		PEStorageSite pesg = (PEStorageSite)pc.getLoaded(ss,getSiteKey(ss.getName()));
		if (pesg == null)
			pesg = new PEStorageSite(ss, pc);
		return pesg;
	}
	
	public String getURL() { return jdbcURL; }

	public String getUser() { return jdbcUser; }

	public String getPassword() { return jdbcPassword; }
	
	public String getHAType() { return haType; }

	public void setHAType(String haType) { this.haType = haType; }
	
	public List<PESiteInstance> getSiteInstances() {
		return siteInstancesToAdd;
	}

	public void addSiteInstance(PESiteInstance siteInstance) {
		if (this.siteInstancesToAdd == null) {
			this.siteInstancesToAdd = new ArrayList<PESiteInstance>();
		}
		this.siteInstancesToAdd.add(siteInstance);
	}

	public void removeSiteInstance(PESiteInstance siteInstance) {
		if (this.siteInstancesToDrop == null) {
			this.siteInstancesToDrop = new ArrayList<PESiteInstance>();
		}
		this.siteInstancesToDrop.add(siteInstance);
	}
	
	public String getDeclarationSQL() {
		StringBuilder buf = new StringBuilder();
		if (this.jdbcURL == null) {
			buf.append(getName().getSQL());
			buf.append(" OF TYPE ");
			buf.append(getHAType());
			if (this.siteInstancesToAdd != null) {
				boolean first = true;
				for(PESiteInstance siteInstance : this.siteInstancesToAdd) {
					if (!first) {
						buf.append(", ");
					}
					buf.append(siteInstance.getName().getUnqualified().get());
					first = false;
				}
			}
		} else {
			buf.append(getName().getSQL()).append(" url='").append(getURL()).append("'")
				.append("user='").append(jdbcUser).append("' password='").append(jdbcPassword).append("'");
		}
		return buf.toString();
	}
	
	@Override
	public String toString() {
		return "PEStorageSite: " + getDeclarationSQL();
	}
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEStorageSite, PersistentSite> other,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEStorageSite opes = other.get();

		if (visited.contains(this) && visited.contains(opes)) {
			return false;
		}
		visited.add(this);
		visited.add(opes);

		if (maybeBuildDiffMessage(sc, messages,"name",name.getSQL(), other.getName().getSQL(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages,"jdbc url", getURL(), opes.getURL(), first, visited))
			return true;
		return false;
	}		
	
	@Override
	public String getDiffTag() {
		return DIFF_TAG;
	}
	
	public static Map<Name, PEStorageSite> buildNameMap(Collection<PEStorageSite> in) {
		return Functional.buildMap(in, new Accessor<Name, PEStorageSite>() {

			@Override
			public Name evaluate(PEStorageSite object) {
				return object.getName();
			}
			
		});
	}

	@Override
	public Persistable<PEStorageSite, PersistentSite> reload(
			SchemaContext usingContext) {
		return usingContext.findStorageSite(getName());
	}

	@Override
	protected int getID(PersistentSite p) {
		return p.getId();
	}

	@Override
	protected PersistentSite lookup(SchemaContext pc) throws PEException {
		String persistentName = name.getUnqualified().get();
		PersistentSite ss = pc.getCatalog().findPersistentSite(persistentName);
		return ss;
	}

	@Override
	protected PersistentSite createEmptyNew(SchemaContext pc) throws PEException {
		PersistentSite ss = null;
		if (this.jdbcURL == null) {
			SiteInstance mSi = (SiteInstance)pc.getBacking(master);
			List<SiteInstance> sis = new ArrayList<SiteInstance>();
			for(PESiteInstance si : this.siteInstancesToAdd) {
				sis.add((SiteInstance)pc.getBacking(si));
			}
			ss = pc.getCatalog().createPersistentSite(name.getUnqualified().get(), 
					haType, mSi, sis.toArray(new SiteInstance[sis.size()]));
		} else {
			ss = pc.getCatalog().createPersistentSite(name.getUnqualified().get(), jdbcURL, jdbcUser, jdbcPassword);
		}
		pc.getSaveContext().add(this,ss);
		return ss;
	}

	@Override
	protected void populateNew(SchemaContext pc, PersistentSite p) throws PEException {
		setBacking(pc,p);
	}

	@Override
	protected Persistable<PEStorageSite, PersistentSite> load(SchemaContext pc, PersistentSite p)
			throws PEException {
		return PEStorageSite.load(p,pc);
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return PersistentSite.class;
	}
	
	@Override
	protected void updateExisting(SchemaContext pc, PersistentSite p) throws PEException {
		setBacking(pc,p);
		super.updateExisting(pc,p);
	}
	
	void setBacking(SchemaContext pc, PersistentSite p) throws PEException {
		addSiteInstances(pc,p);
		
		if (haType != null) {
			p.setHaType(haType);
		}
		
		if (master != null) {
			SiteInstance mSi = (SiteInstance)pc.getBacking(master);
			p.setMasterInstance(mSi);
		}
	}
	
	void addSiteInstances(SchemaContext pc, PersistentSite p) throws PEException {
		if (this.siteInstancesToAdd != null) {
			HashSet<SiteInstance> already = new HashSet<SiteInstance>(p.getSiteInstances());
			for(PESiteInstance pes : this.siteInstancesToAdd) {
				SiteInstance ss = pes.persistTree(pc);
				if (!already.contains(ss))
					p.addInstance(ss);
			}
		}
		if (this.siteInstancesToDrop != null) {
			HashSet<SiteInstance> already = new HashSet<SiteInstance>(p.getSiteInstances());
			for(PESiteInstance pes : this.siteInstancesToDrop) {
				SiteInstance ss = pes.persistTree(pc);
				ss.clearStorageSite();
				if (already.contains(ss))
					p.removeInstance(ss);
				pc.persist(ss);
			}
		}
	}
	
	public void setMaster(PESiteInstance newMaster) {
		this.master = newMaster;
	}
	
	public static SchemaCacheKey<PEStorageSite> getSiteKey(Name n) {
		return getSiteKey(n.getUnqualified().getUnquotedName().get());
	}
	
	public static SchemaCacheKey<PEStorageSite> getSiteKey(String n) {
		return new StorageSiteCacheKey(n);
	}
	
	public static class StorageSiteCacheKey extends SchemaCacheKey<PEStorageSite> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final String name;
		
		public StorageSiteCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public int hashCode() {
			return initHash(PEStorageSite.class, name.hashCode());
		}
		
		@Override
		public String toString() {
			return "PEStorageSite:" + name;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof StorageSiteCacheKey) {
				StorageSiteCacheKey ssk = (StorageSiteCacheKey) o;
				return name.equals(ssk.name);
			}
			return false;
		}

		@Override
		public PEStorageSite load(SchemaContext sc) {
			PersistentSite ss = sc.getCatalog().findPersistentSite(name);
			if (ss == null)
				return null;
			return PEStorageSite.load(ss, sc);
		}

		@Override
		public Collection<SchemaCacheKey<?>> getCascades(Object obj) {
			SchemaCacheKey<?> theKey = AllPersistentSitesCacheKey.key;
			ArrayList<SchemaCacheKey<?>> one = new ArrayList<SchemaCacheKey<?>>();
			one.add(theKey);
			return one;
		}

	}

	public PESiteInstance getMaster() {
		return master;
	}
	
	
	public StorageSite getTCache() {
		return new TCacheSite(this);
	}
	
	public static class TCacheSite implements StorageSite {

		private PEStorageSite pess;
		
		public TCacheSite(PEStorageSite p) {
			pess = p;
		}
		
		@Override
		public String getName() {
			return pess.getName().get();
		}

		@Override
		public String getMasterUrl() {
			if (pess.getMaster() == null)
				return null;
			return pess.getMaster().getUrl();
		}

		private Worker.Factory getWorkerFactory() {
			return PersistentSite.getWorkerFactory(pess.getHAType());
		}
		
		@Override
		public String getInstanceIdentifier() {
			return getWorkerFactory().getInstanceIdentifier(this, pess.getMaster().getTCache());
		}

		@Override
		public PersistentSite getRecoverableSite(CatalogDAO c) {
			PersistentSite ps = c.findByKey(pess.getPersistentClass(), pess.getPersistentID());
			return ps;
		}

		@Override
		public Worker pickWorker(Map<StorageSite, Worker> workerMap)
				throws PEException {
			return workerMap.get(this);
		}
		
		@Override
		public String toString() {
			return "TCacheSite:" + getName();
		}

		@Override
		public void annotateStatistics(LogSiteStatisticRequest sNotice) {
			sNotice.setSiteDetails(getName(), SiteType.PERSISTENT);
		}

		@Override
		public void incrementUsageCount() {
		}

		@Override
		public Worker createWorker(UserAuthentication auth) throws PEException {
			if(auth.isAdminUser())
				return getWorkerFactory().newWorker(pess.getMaster().getAuthentication(), this);

			return getWorkerFactory().newWorker(auth, this);
		}

		@Override
		public void onSiteFailure(CatalogDAO c) throws PEException {
			getRecoverableSite(c).onSiteFailure(c);
		}
		
		@Override
		public int hashCode() {
			return PersistentSite.computeHashCode(this);
		}
		
		@Override
		public boolean equals(Object other) {
			return PersistentSite.computeEquals(this, other);
		}

		@Override
		public int getMasterInstanceId() {
			return 0;
		}

		@Override
		public boolean supportsTransactions() {
			return true;
		}

		@Override
		public boolean hasDatabase(UserVisibleDatabase ctxDB) {
			return true;
		}

		@Override
		public void setHasDatabase(UserVisibleDatabase ctxDB) throws PEException {
			throw new PECodingException("Invalid call to StorageSite.setHasDatabase");
		}
				
	}

	public static class AllPersistentSitesCacheKey extends SchemaCollectionCacheKey<PEStorageSite> {

		private static final long serialVersionUID = 1L;
		// only one of these
		protected static final AllPersistentSitesCacheKey key = new AllPersistentSitesCacheKey();
		private static final String name = "AllPersistentSites"; 
		
		private AllPersistentSitesCacheKey() {
			super();
		}
		
		public static Collection<PEStorageSite> get(SchemaContext sc) {
			return key.resolve(sc);
		}
		
		@Override
		public int hashCode() {
			return addHash(Collection.class.hashCode(),addHash(PEStorageSite.class.hashCode(),name.hashCode()));
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof AllPersistentSitesCacheKey;
		}

		@Override
		public Collection<PEStorageSite> find(final SchemaContext sc) {
			List<PersistentSite> sites = sc.getCatalog().findAllSites();
			return Functional.apply(sites, new UnaryFunction<PEStorageSite,PersistentSite>() {

				@Override
				public PEStorageSite evaluate(PersistentSite object) {
					return PEStorageSite.load(object, sc);
				}
				
			});
		}

		@Override
		public String toString() {
			return name;
		}
		
	}

}
