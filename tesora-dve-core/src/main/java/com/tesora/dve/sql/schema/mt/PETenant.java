// OS_STATUS: public
package com.tesora.dve.sql.schema.mt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ITenant;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.MTTableInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheAwareLookup;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.NamedEdge;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.TableScope.ScopeCacheKey;

public class PETenant extends Persistable<PETenant, Tenant> implements IPETenant {

	private String extID;
	private String description;
	private boolean suspended;
	private long tenantID;

	private SchemaEdge<PEDatabase> ofDB;
	
	private CacheAwareLookup<TableScope> lookup;
	private boolean loaded = false;

	public PETenant(SchemaContext pc, PEDatabase onDatabase, String extID, String desc) {
		this(pc, onDatabase, extID, desc, -1);
	}
	
	// used in the tests only!
	@SuppressWarnings("unchecked")
	public PETenant(SchemaContext pc, PEDatabase onDatabase, String extID, String desc, int requestedID) {
		super(getTenantKey(extID,requestedID));
		this.extID = extID;
		this.description = desc;
		this.suspended = false;
		this.tenantID = requestedID;
		this.ofDB = StructuralUtils.buildEdge(pc,onDatabase,false);
		if (requestedID != -1 && pc.getCatalog().isPersistent())
			throw new SchemaException(Pass.SECOND, "Wrong ctor for PETenant");
        lookup = Singletons.require(HostService.class).getDBNative().getEmitter().getTenantTableLookup();
		loaded = true;
	}

	public String getExternalID() { return extID; }
	
	@Override
	public Name getName() { return new UnqualifiedName(extID); }
	
	@Override
	public String getUniqueIdentifier() { return getName().get(); }
	
	public String getDescription() { return description; }
	public boolean isSuspended() { return suspended; }
	public void setSuspended(boolean v) { suspended = v; }

	public PEDatabase getDatabase(SchemaContext sc) { return ofDB.get(sc); }
	
	public TableScope lookupScope(SchemaContext pc, Name n, LockInfo lockInfo) {
		return lazyLookupScope(pc, n, lockInfo);
	}
	
	private TableScope lazyLookupScope(SchemaContext pc, Name n, LockInfo lockType) {
		TableScope targ = lookup.lookup(pc, n);
		if (targ == null) {
			ScopeCacheKey sck = TableScope.getScopeKey(this, n);
			sck.acquireLock(pc, lockType);
			targ = pc.findScope(sck);
			if (targ == null) return null;
			lookup.add(pc, targ,true);
		} else {
			targ.getCacheKey().acquireLock(pc, lockType);
		}
			
		return targ;
	}
	
	public PETable lookup(SchemaContext pc, Name n, LockInfo info) {
		TableScope ts = lookupScope(pc,n, info);
		if (ts == null) return null;
		return ts.getTable(pc);
	}
	
	public TableInstance build(SchemaContext pc, Name n, LockInfo info) {
		TableScope ts = lookupScope(pc,n, info);
		if (ts == null) return null;
		return new MTTableInstance(ts.getTable(pc),ts,pc.getOptions().isResolve());
	}
	
	public TableScope setVisible(SchemaContext sc, PETable tab, Name localName, Long autoIncOffset, LockInfo info) {
		TableScope already = lookupScope(sc, localName, info);
		if (already != null) return null;
		TableScope ts = new TableScope(sc, tab, this, autoIncOffset, localName);
		lookup.add(sc, ts,false);
		return ts;
	}
	
	public Collection<TableScope> getTableScopes(SchemaContext sc) {
		checkLoaded(sc);
		ArrayList<TableScope> scopes = new ArrayList<TableScope>();
		for(NamedEdge<TableScope> ne : lookup.getValues()) {
			TableScope ts = ne.getEdge().get(sc);
			if (ts == null) continue;
			scopes.add(ts);
		}
		return scopes;
	}
	
	@Override
	public Long getTenantID() {
		return new Long(tenantID);
	}

	@Override
	public boolean isGlobalTenant() {
		return PEConstants.LANDLORD_TENANT.equals(extID);
	}

	@SuppressWarnings("unchecked")
	private PETenant(Tenant ten, SchemaContext sc) {
		super(getTenantKey(ten));
		sc.startLoading(this, ten);
		setPersistent(sc,ten,ten.getId());
		this.extID = ten.getExternalTenantId();
		this.description = ten.getDescription();
		this.suspended = ten.isSuspended();
		this.tenantID = ten.getId();
		this.ofDB = StructuralUtils.buildEdge(sc,PEDatabase.load(ten.getDatabase(), sc),true);
        this.lookup = Singletons.require(HostService.class).getDBNative().getEmitter().getTenantTableLookup();
		this.loaded = false;
		sc.finishedLoading(this, ten);
	}
	
	private void checkLoaded(SchemaContext pc) {
		if (this.loaded) return;
		this.loaded = true;
		if (pc.isPersistent()) {
			for(TableVisibility tv : getPersistent(pc).getScoping()) {
				TableScope ts = TableScope.load(tv, pc);
				lookup.add(pc,ts,true);
			}
		}
	}
	
	public static PETenant load(Tenant ten, SchemaContext sc) {
		PETenant peu = (PETenant)sc.getLoaded(ten,getTenantKey(ten));
		if (peu == null)
			peu = new PETenant(ten, sc);
		return peu;
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return Tenant.class;
	}

	@Override
	protected Tenant lookup(SchemaContext pc) throws PEException {
		if (!pc.isPersistent()) return null;
		return pc.getCatalog().findTenant(extID);
	}

	@Override
	protected Tenant createEmptyNew(SchemaContext pc) throws PEException {
		return new Tenant(ofDB.get(pc).persistTree(pc), extID, description);
	}

	@Override
	protected void populateNew(SchemaContext pc, Tenant p) throws PEException {
		if (suspended)
			p.setSuspended();
		else
			p.setResumed();
		
	}

	@Override
	public ITenant getPersistentTenant(SchemaContext sc) throws PEException {
		return getPersistent(sc);
	}
	
	@Override
	protected void updateExisting(SchemaContext pc, Tenant p) throws PEException {
		populateNew(pc, p);
	}
	
	@Override
	protected Persistable<PETenant, Tenant> load(SchemaContext pc, Tenant p) throws PEException {
		return null;
	}

	@Override
	protected int getID(Tenant p) {
		return p.getId();
	}

	@Override
	protected String getDiffTag() {
		return null;
	}

	public UnqualifiedName buildPrivateTableName(SchemaContext pc, Name n) {
		List<String> existingNames = pc.findTenantTableNames(this);
		HashSet<String> such = new HashSet<String>(existingNames);
		StringBuilder buf = new StringBuilder();
		buf.append("_").append(tenantID).append(n.get());
		UnqualifiedName candidate = new UnqualifiedName(buf.toString(),n.isQuoted()); 
		String nn = null;
		int pcounter = 0;
		while(nn == null) {
			nn = candidate.getUnqualified().get() + pcounter;
			if (such.contains(nn)) {
				pcounter++;
				nn = null;
			}
		}
		return new UnqualifiedName(nn);
	}
	
	public static SchemaCacheKey<PETenant> getTenantKey(Name tenantName) {
		return getTenantKey(tenantName.getUnquotedName().get(),-1);
	}
	
	public static SchemaCacheKey<PETenant> getTenantKey(String n, int tenantID) {
		return new TenantCacheKey(n,tenantID);
	}
	
	public static SchemaCacheKey<PETenant> getTenantKey(Tenant tenant) {
		return getTenantKey(tenant.getExternalTenantId(),tenant.getId());
	}
	
	public static class TenantCacheKey extends SchemaCacheKey<PETenant> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final String tenantName;
		private final int tenantID;
		
		public TenantCacheKey(String n, int tenID) {
			super();
			tenantName = n;
			tenantID = tenID;
		}
		
		@Override
		public int hashCode() {
			return initHash(PETenant.class,tenantName.hashCode());
		}
		
		@Override
		public String toString() {
			return "PETenant:" + tenantName;
		}
		
		public String getTenantName() {
			return tenantName;
		}
		
		public int getTenantID() {
			return tenantID;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof TenantCacheKey) {
				TenantCacheKey oth = (TenantCacheKey) o;
				return tenantName.equals(oth.tenantName);
			}
			return false;
		}

		@Override
		public PETenant load(SchemaContext sc) {
			Tenant ten = null;
			if (sc.getCatalog().isPersistent() && tenantID > 0)
				ten = (Tenant) sc.getCatalog().findByKey(Tenant.class,tenantID);
			else 
				ten = sc.getCatalog().findTenant(tenantName);
			if (ten == null) return null;
			return PETenant.load(ten, sc);
		}		
		
		@Override
		public CacheSegment getCacheSegment() {
			return CacheSegment.TENANT;
		}

		@Override
		public Collection<SchemaCacheKey<?>> getCascades(Object ten) {
			PETenant tenant = (PETenant) ten;
			return tenant.lookup.getCascades();
		}

	}

}
