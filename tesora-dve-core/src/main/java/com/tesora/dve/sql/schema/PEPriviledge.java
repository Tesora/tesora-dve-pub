// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Priviledge;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.mt.PETenant;

public class PEPriviledge extends Persistable<PEPriviledge, Priviledge> {

	private final PEUser forUser;
	private PEDatabase onDatabase;
	private PETenant onTenant;
	
	public PEPriviledge(PEUser user, PEDatabase db) {
		this(user,db,null);
	}
	
	public PEPriviledge(PEUser user, PETenant tenant) {
		this(user,null,tenant);
	}
	
	private PEPriviledge(PEUser user, PEDatabase db, PETenant tenant) {
		super(null);
		forUser = user;
		onDatabase = db;
		onTenant = tenant;
		setName(null);
	}

	private PEPriviledge(SchemaContext pc, Priviledge p) {
		super(getPriviledgeKey(p));
		pc.startLoading(this,p);
		forUser = PEUser.load(p.getUser(),pc);
		onDatabase = null;
		onTenant = null;
		if (p.getDatabase() != null) onDatabase = PEDatabase.load(p.getDatabase(), pc);
		if (p.getTenant() != null) onTenant = PETenant.load(p.getTenant(), pc);
		setPersistent(pc,p,p.getId());
		pc.finishedLoading(this,p); 
	}
	
	public static PEPriviledge load(Priviledge p, SchemaContext sc) {
		PEPriviledge pep = (PEPriviledge) sc.getLoaded(p,getPriviledgeKey(p));
		if (pep == null)
			pep = new PEPriviledge(sc, p);
		return pep;
	}
	
	public PEUser getUser() { return forUser; }
	public PEDatabase getDatabase() { return onDatabase; }
	public PETenant getTenant() { return onTenant; }
	
	public boolean isGlobal() { return onDatabase == null && onTenant == null; }
	
	public GrantScope getGrantScope() {
		if (onDatabase != null)
			return new GrantScope(onDatabase);
		else
			return new GrantScope(onTenant);
	}
	
	public Persistable<?,?> match(Name dbName) {
		if (onDatabase != null && onDatabase.getName().equals(dbName))
			return onDatabase;
		if (onTenant != null && onTenant.getName().equals(dbName))
			return onTenant;
		return null;
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return Priviledge.class;
	}

	@Override
	protected Priviledge lookup(SchemaContext pc) throws PEException {
		User user = forUser.persistTree(pc);
		return user.findPriviledge((onDatabase == null ? null : onDatabase.persistTree(pc)),
				(onTenant == null ? null : onTenant.persistTree(pc)));
	}

	@Override
	protected Priviledge createEmptyNew(SchemaContext pc) throws PEException {
		if (onDatabase != null)
			return new Priviledge(forUser.persistTree(pc), onDatabase.persistTree(pc));
		else if (onTenant != null)
			return new Priviledge(forUser.persistTree(pc), onTenant.persistTree(pc));
		else
			return new Priviledge(forUser.persistTree(pc));
	}

	@Override
	protected void populateNew(SchemaContext pc, Priviledge p) throws PEException {
	}

	@Override
	protected Persistable<PEPriviledge, Priviledge> load(SchemaContext pc, Priviledge p)
			throws PEException {
		return null;
	}

	@Override
	protected int getID(Priviledge p) {
		return p.getId();
	}

	@Override
	protected String getDiffTag() {
		return null;
	}

	protected SchemaCacheKey<?> _buildCacheKey(SchemaContext sc) {
		if (onDatabase == null && onTenant == null) return null;
		return getPriviledgeKey(forUser,(onDatabase == null ? onTenant.getName() : onDatabase.getName()));
	}

	public static SchemaCacheKey<PEPriviledge> getPriviledgeKey(Priviledge p) {
		String privname = null;
		if (p.getDatabase() == null && p.getTenant() == null)
			privname = null;
		else if (p.getDatabase() != null)
			privname = p.getDatabase().getName();
		else
			privname = p.getTenant().getExternalTenantId();
		return getPriviledgeKey(p.getUser(),privname);
	}
	
	public static SchemaCacheKey<PEPriviledge> getPriviledgeKey(PEUser user, Name n) {
		if (user.getPersistentID() == null) return null;
		return new PrivilegeCacheKey(user.getPersistentID(),n.getUnquotedName().get());
	}
	
	public static SchemaCacheKey<PEPriviledge> getPriviledgeKey(User user, String n) {
		if (user.getId() == 0) return null;
		return new PrivilegeCacheKey(user.getId(),n);
	}
	
	public static class PrivilegeCacheKey extends SchemaCacheKey<PEPriviledge> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final int userid;
		private final String onName;
		
		public PrivilegeCacheKey(int userid, String n) {
			super();
			this.userid = userid;
			this.onName = n;
		}
		
		@Override
		public int hashCode() {
			return addIntHash(initHash(PEPriviledge.class, (onName == null ? 0 : onName.hashCode())),userid);
		}
		
		@Override
		public String toString() {
			return "PEPriviledge:" + userid + "/" + onName;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof PrivilegeCacheKey) {
				PrivilegeCacheKey psk = (PrivilegeCacheKey) o;
				return this.userid == psk.userid && 
						((this.onName == null && psk.onName == null)
						 || 	this.onName.equals(psk.onName));
			}
			return false;
		}

		@Override
		public PEPriviledge load(SchemaContext sc) {
			Priviledge p = sc.getCatalog().findPrivilege(sc, userid, onName); 
			if (p == null) return null;
			return PEPriviledge.load(p,sc);
		}
		
	}
	
}
