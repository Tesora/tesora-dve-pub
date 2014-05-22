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


import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaCollectionCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdgeList;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.worker.agent.Agent;
import com.tesora.dve.worker.WorkerManager;

public class PEPersistentGroup extends Persistable<PEPersistentGroup, PersistentGroup> 
	implements PEStorageGroup {

	private final SchemaEdgeList<PEStorageSite> sites;
	// storage gen add
	private int version;
	private boolean dirty;
	private final PersistentGroup tempGroup;
	
	public PEPersistentGroup(SchemaContext pc, Name n, List<PEStorageSite> storageSites) {
		super(getGroupKey(n));
		setName(n);
		sites = new SchemaEdgeList<PEStorageSite>();
		for(PEStorageSite pess : storageSites)
			sites.add(pc,pess,false);
		dirty = true;
		tempGroup = null;
		version = 0;
		setPersistent(pc,null,null);
	}

	@Override
	public String toString() {
		return "PEStorageGroup/" + getName();
	}

	public static PEPersistentGroup load(PersistentGroup sg, SchemaContext pc) {
		return load(sg,pc,false);
	}
	
	public static PEPersistentGroup load(PersistentGroup sg, SchemaContext lc, boolean temp) {
		PEPersistentGroup pesg = (PEPersistentGroup) lc.getLoaded(sg, getGroupKey(sg.getName()));
		if (pesg == null) 
			pesg = new PEPersistentGroup(sg, lc, temp);
		return pesg;
	}
	
	private PEPersistentGroup(PersistentGroup sg, SchemaContext lc, boolean temp) {
		super(getGroupKey(sg.getName()));
		if (!temp) 
			lc.startLoading(this,sg);
		UnqualifiedName pgn = new UnqualifiedName(sg.getName());
		tempGroup = sg;
		setName(pgn);
		sites = new SchemaEdgeList<PEStorageSite>();
		for(PersistentSite s: sg.getStorageSites()) {
			sites.add(lc,PEStorageSite.load(s, lc),true);
		}
		version = sg.getLastGen().getVersion();
		setPersistent(lc,sg,sg.getId());
		dirty = false;
		if (!temp)
			lc.finishedLoading(this, sg);
	}
	
	public List<PEStorageSite> getSites(SchemaContext sc) { 
		return sites.resolve(sc);
	}

	public int getVersion() {
		return version;
	}
	
	public void addSite(SchemaContext sc, PEStorageSite pess, boolean persistent) {
		sites.add(sc,pess,persistent);
		dirty = true;
	}
	
	@Override
	public boolean comparable(SchemaContext sc, PEStorageGroup other) {
		if (other instanceof PEPersistentGroup) {
			if (sc.getCatalog().isPersistent()) {
				PersistentGroup backing = getPersistent(sc);
				PersistentGroup otherBacking = (PersistentGroup) other.getPersistent(sc);
				if (backing == null && otherBacking == null) {
					return name.equals(((PEPersistentGroup)other).name);
				} else if (backing != null && otherBacking != null) {
					return backing.getId() == otherBacking.getId();
				} else {
					return false;
				}			
			} else {
				return name.equals(((PEPersistentGroup)other).name);
			}
		} return false;
	}

	@Override
	public boolean isSubsetOf(SchemaContext sc, PEStorageGroup other) {
		if (other instanceof PEPersistentGroup) {
			PEPersistentGroup opeg = (PEPersistentGroup) other;
			List<PEStorageSite> mySites = getSites(sc);
			List<PEStorageSite> yourSites = opeg.getSites(sc);
			if (yourSites.size() > mySites.size())
				return yourSites.containsAll(mySites);
			else
				return mySites.containsAll(yourSites);
		}
		return false;
	}
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages,
			Persistable<PEPersistentGroup, PersistentGroup> oth, boolean first,
			@SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEPersistentGroup other = oth.get();
		
		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);
		
		if (maybeBuildDiffMessage(sc, messages,"name",getName(), other.getName(), first, visited))
			return true;
		Map<Name, PEStorageSite> currentSites = PEStorageSite.buildNameMap(getSites(sc)); 
		Map<Name, PEStorageSite> otherSites = PEStorageSite.buildNameMap(other.getSites(sc)); 
		if (maybeBuildDiffMessage(sc, messages,PEStorageSite.DIFF_TAG,currentSites, otherSites, first, visited))
			return true;
		return false;
	}

	@Override
	protected String getDiffTag() {
		return "Persistent group";
	}

	@Override
	public Persistable<PEPersistentGroup, PersistentGroup> reload(
			SchemaContext usingContext) {
		return usingContext.findStorageGroup(getName());
	}
		
	@Override
	public PEPersistentGroup anySite(SchemaContext pc) throws PEException {
		if (pc.isPersistent()) {
			return load(getPersistent(pc).anySite(),pc,true);
		} else {
			return aSite(pc,Agent.getRandom(sites.size()));
		}
	}	
	
	@Override
	public PersistentGroup getPersistent(SchemaContext pc, boolean create) {
		if (tempGroup != null) return tempGroup;
		return super.getPersistent(pc, false);
	}
	
	public PEPersistentGroup aSite(SchemaContext pc, int mem) throws PEException {
		if (pc.isPersistent())
			throw new PEException("PEStorageGroup.aSite is for tschema only!");
		PEStorageSite site = sites.get(mem).get(pc);
		PEPersistentGroup ret = new PEPersistentGroup(pc,new UnqualifiedName("TempGroup"),Collections.singletonList(site));
		// make sure there is a backing StorageGroup for the tests
		pc.beginSaveContext();
		try {
			pc.setMutableSourceOverride(true);
			ret.persistTree(pc);
		} finally {
			pc.setMutableSourceOverride(null);
			pc.endSaveContext();
		}
		return ret;		
	}
	
	@Override
	protected boolean isTemporary() {
		return tempGroup != null;
	}
	

	
	@Override
	protected int getID(PersistentGroup p) {
		return p.getId();
	}

	@Override
	protected PersistentGroup lookup(SchemaContext pc) throws PEException {
		return pc.getCatalog().findPersistentGroup(name.getUnqualified().get());
	}

	@Override
	protected PersistentGroup createEmptyNew(SchemaContext pc) throws PEException {
		PersistentGroup sg = pc.getCatalog().createPersistentGroup(name.getUnqualified().get());
		if (!isTemporary())
			pc.getSaveContext().add(this,sg);
		return sg;
	}

	@Override
	protected void populateNew(SchemaContext pc,PersistentGroup p) throws PEException {
		updateExisting(pc,p);
	}

	@Override
	protected void updateExisting(SchemaContext pc, PersistentGroup p) throws PEException {
		if (dirty) {
			dirty = false;
			HashSet<PersistentSite> already = new HashSet<PersistentSite>(p.getStorageSites());
			for(PEStorageSite pes : getSites(pc)) {
				PersistentSite ss = pes.persistTree(pc);
				if (!already.contains(ss))
					p.addStorageSite(ss);
			}
		}		
	}
	
	@Override
	protected Persistable<PEPersistentGroup, PersistentGroup> load(SchemaContext pc, PersistentGroup p)
			throws PEException {
		return PEPersistentGroup.load(p,pc);
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return PersistentGroup.class;
	}

	@Override
	public boolean isSingleSiteGroup() {
		return sites.size() == 1;
	}

	@Override
	public void setCost(int score) throws PEException {
	}

	@Override
	public boolean isTempGroup() {
		return false;
	}

	@Override
	public PEStorageGroup getPEStorageGroup(SchemaContext sc) {
		return this;
	}
	
	public static SchemaCacheKey<PEPersistentGroup> getGroupKey(Name n) {
		return getGroupKey(n.getUnquotedName().getUnqualified().get());
	}
	
	public static SchemaCacheKey<PEPersistentGroup> getGroupKey(String n) {
		return new PersistentGroupCacheKey(n);
	}
	
	public static class PersistentGroupCacheKey extends SchemaCacheKey<PEPersistentGroup> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		// by name
		private final String name;
		
		public PersistentGroupCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public int hashCode() {
			return initHash(PEPersistentGroup.class, name.hashCode());
		}
		
		@Override
		public String toString() {
			return "PEPersistentGroup:" + name;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof PersistentGroupCacheKey) {
				PersistentGroupCacheKey pgk = (PersistentGroupCacheKey) o;
				return name.equals(pgk.name);
			}
			return false;
		}

		@Override
		public PEPersistentGroup load(SchemaContext sc) {
			PersistentGroup sg = sc.getCatalog().findPersistentGroup(name);
			if (sg == null)
				return null;
			return PEPersistentGroup.load(sg, sc);
		}

	}
	
	private static final boolean caching = !Boolean.getBoolean("com.tesora.dve.sql.schema.tcache.storagegroup.disable");
	
	@Override
	public StorageGroup getScheduledGroup(SchemaContext sc) {
		if (caching) {
			return new TStorageGroup(this,getSites(sc));
		} else
			return getPersistent(sc);
	}
	
	public static class TStorageGroup implements StorageGroup {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final PEPersistentGroup peg;
		private final List<PEStorageSite> resolved;
		private List<StorageSite> cachedSites = null;
		
		public TStorageGroup(PEPersistentGroup parent, List<PEStorageSite> resolved) {
			peg = parent;
			this.resolved = resolved;
		}
		
		@Override
		public String getName() {
			return peg.getName().get();
		}

		@Override
		public int sizeForProvisioning() throws PEException {
			return peg.sites.size();
		}

		private List<StorageSite> getCachedSites() {
			if (cachedSites == null) {
				cachedSites = Functional.apply(resolved, new UnaryFunction<StorageSite,PEStorageSite>() {

					@Override
					public StorageSite evaluate(PEStorageSite object) {
						return object.getTCache();
					}
					
				});
			}
			return cachedSites;
		}
		
		@Override
		public void provisionGetWorkerRequest(GetWorkerRequest getWorkerRequest)
				throws PEException {
			getWorkerRequest.fulfillGetWorkerRequest(getCachedSites());
		}

		@Override
		public void returnWorkerSites(WorkerManager workerManager,
				Collection<? extends StorageSite> groupSites)
				throws PEException {
		}

		@Override
		public boolean isTemporaryGroup() {
			return PersistentGroup.TEMP_NAME.equals(getName());
		}
		
		@Override
		public int hashCode() {
			return PersistentGroup.computeHashCode(this);
		}
		
		@Override
		public boolean equals(Object other) {
			return PersistentGroup.computeEquals(this, other);
		}
		
		@Override
		public String toString() {
			return getName();
		}

		@Override
		public int getId() {
			return peg.getPersistentID();
		}
	}
	
	public static class AllPersistentGroupsCacheKey extends SchemaCollectionCacheKey<PEPersistentGroup> {

		private static final long serialVersionUID = 1L;
		protected static final AllPersistentGroupsCacheKey key = new AllPersistentGroupsCacheKey();
		private static final String name = "AllPersistentGroups"; 
		
		private AllPersistentGroupsCacheKey() {
			super();
		}
		
		public static Collection<PEPersistentGroup> get(SchemaContext sc) {
			return key.resolve(sc);
		}
		
		@Override
		public int hashCode() {
			return addHash(Collection.class.hashCode(),addHash(PEPersistentGroup.class.hashCode(),name.hashCode()));
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof AllPersistentGroupsCacheKey;
		}

		@Override
		public Collection<PEPersistentGroup> find(final SchemaContext sc) {
			List<PersistentGroup> groups = sc.getCatalog().findAllGroups();
			return Functional.apply(groups, new UnaryFunction<PEPersistentGroup,PersistentGroup>() {

				@Override
				public PEPersistentGroup evaluate(PersistentGroup object) {
					return PEPersistentGroup.load(object, sc);
				}
			});
		}

		@Override
		public String toString() {
			return name;
		}
	}
}
