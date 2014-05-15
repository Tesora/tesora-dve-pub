// OS_STATUS: public
package com.tesora.dve.sql.schema;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.worker.UserAuthentication;

public class PEUser extends Persistable<PEUser, User> {

	private final UserScope name;
	private final boolean root;
	private final List<SchemaEdge<PEPriviledge>> priviledges;
	private Boolean globalPriviledge = null;
	private UserAuthentication authentication;
	
	public PEUser(UserScope n, String pword, boolean adminUser) {
		super(getUserKey(n.getUserName(),n.getScope()));
		name = n;
		authentication = new UserAuthentication(n.getUserName(),pword, adminUser);
		root = false;
		priviledges = new ArrayList<SchemaEdge<PEPriviledge>>(0);
	}
	
	// only used in testing
	public PEUser(SchemaContext sc) {
		super(getUserKey(PEConstants.ROOT, PEConstants.LOCALHOST));
		if (sc.isPersistent())
			throw new IllegalStateException("Invalid ctor for PEUser");
		name = new UserScope(PEConstants.ROOT, PEConstants.LOCALHOST);
		authentication = new UserAuthentication(PEConstants.ROOT, PEConstants.PASSWORD, true);
		root = true;
		priviledges = new ArrayList<SchemaEdge<PEPriviledge>>(0);
	}
	
	public UserScope getUserScope() { return name; }
	public UserAuthentication getUserAuthentication() {
		return authentication;
	}
	public String getPassword() {
		return authentication.getPassword();
	}

	public boolean isRoot() { return root; }
	
	public void setPassword(String v) {
		authentication = new UserAuthentication(name.getUserName(),v, authentication.isAdminUser());
	}
	
	private PEUser(User us, SchemaContext pc) {
		super(getUserKey(us.getName(),us.getAccessSpec()));
		pc.startLoading(this,us);
		authentication = new UserAuthentication(us.getName(), us.getPlaintextPassword(),us.getAdminUser());
		name = new UserScope(us.getName(),us.getAccessSpec());
		root = us.getAdminUser();
		priviledges = new ArrayList<SchemaEdge<PEPriviledge>>();
		setPersistent(pc, us, us.getId());
		pc.finishedLoading(this, us);
	}
	
	public static PEUser load(User us, SchemaContext pc) {
		PEUser peu = (PEUser)pc.getLoaded(us,getUserKey(us.getName(),us.getAccessSpec()));
		if (peu == null)
			peu = new PEUser(us, pc);
		return peu;
	}
	
	@SuppressWarnings("unchecked")
	public Persistable<?,?> resolve(SchemaContext sc, Name dbName) {
		for(Iterator<SchemaEdge<PEPriviledge>> iter = priviledges.iterator(); iter.hasNext();) {
			SchemaEdge<PEPriviledge> sck = iter.next();
			PEPriviledge pep = sck.get(sc);
			if (pep == null)
				iter.remove();
			else {
				Persistable<?,?> any = pep.match(dbName);
				if (any != null)
					return any;
			}
		}
		// not found amongst loaded privs, go back to the catalog
		PEPriviledge pep = sc.findPrivilege(this,dbName);
		if (pep != null) {
			priviledges.add(StructuralUtils.buildEdge(sc,pep, true));
			return pep.match(dbName);
		}
		return null;
	}
	
	public boolean hasGlobalPriviledge(SchemaContext sc) {
		if (globalPriviledge == null) {
			for(Iterator<SchemaEdge<PEPriviledge>> iter = priviledges.iterator(); iter.hasNext();) {
				SchemaEdge<PEPriviledge> sck = iter.next();
				PEPriviledge pep = sck.get(sc);
				if (pep == null)
					iter.remove();
				else if (pep.isGlobal()) {
					globalPriviledge = true;
					return true;
				}
			}
			globalPriviledge = sc.getCatalog().findGlobalPrivilege(sc,this);
		}		
		return globalPriviledge.booleanValue();
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return User.class;
	}

	@Override
	protected User lookup(SchemaContext pc) throws PEException {
		List<User> users = pc.getCatalog().findUsers(name.getUserName(),name.getScope());
		if (users.size() > 1)
			throw new PEException("More than one user matching '" + name.getSQL() + "' found");
		if (users.isEmpty())
			return null;
		return users.get(0);
	}

	@Override
	protected User createEmptyNew(SchemaContext sc) throws PEException {
		return new User(name.getUserName(), authentication.getPassword(), name.getScope());
	}

	@Override
	protected void populateNew(SchemaContext pc, User p) throws PEException {
		for(SchemaEdge<PEPriviledge> pep : priviledges) {
			p.addPriviledge(pep.get(pc).persistTree(pc));
		}
	}

	@Override
	protected Persistable<PEUser, User> load(SchemaContext pc, User p) throws PEException {
		return new PEUser(p,pc);
	}

	@Override
	protected int getID(User p) {
		return p.getId();
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages,
			Persistable<PEUser, User> other, boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		return false;
	}

	@Override
	protected String getDiffTag() {
		return null;
	}
	
	@Override
	protected void updateExisting(SchemaContext pc, User p) throws PEException {
		p.setPlaintextPassword(authentication.getPassword());
		for(SchemaEdge<PEPriviledge> e : priviledges) {
			PEPriviledge pep = e.get(pc);
			if (pep.getPersistentID() == null) {
				p.addPriviledge(pep.persistTree(pc));
			}
		}
	}
	
	public static SchemaCacheKey<PEUser> getUserKey(String name, String accessSpec) {
		return new UserCacheKey(name,accessSpec);
	}
	
	public static SchemaCacheKey<PEUser> getUserKey(UserScope scope) {
		return new UserCacheKey(scope.getUserName(),scope.getScope());
	}

	public static SchemaCacheKey<PEUser> getUserKey(User u) {
		return new UserCacheKey(u.getName(),u.getAccessSpec());
	}
	
	public static class UserCacheKey extends SchemaCacheKey<PEUser> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String name;
		private String access;
		
		public UserCacheKey(String name, String access) {
			super();
			this.name = name;
			this.access = access;
		}
		
		@Override
		public int hashCode() {
			return addHash(initHash(PEUser.class,name.hashCode()),access.hashCode());
		}
		
		@Override
		public String toString() {
			return "PEUser:" + name + "@" + access;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof UserCacheKey) {
				UserCacheKey ulk = (UserCacheKey) o;
				return this.name.equals(ulk.name) && this.access.equals(ulk.access);
			}
			return false;
		}

		@Override
		public PEUser load(SchemaContext sc) {
			List<User> users = sc.getCatalog().findUsers(name, access);
			if (users.isEmpty()) return null;
			if (users.size() > 1)
				throw new SchemaException(Pass.SECOND,"Multiple users matching '" + name + "@'" + access + "' found");
			return PEUser.load(users.get(0), sc);
		}

		@Override
		public Collection<SchemaCacheKey<?>> getCascades(Object obj) {
			PEUser user = (PEUser) obj;
			return Functional.apply(user.priviledges, new UnaryFunction<SchemaCacheKey<?>,SchemaEdge<PEPriviledge>>() {

				@Override
				public SchemaCacheKey<?> evaluate(SchemaEdge<PEPriviledge> object) {
					return object.getCacheKey();
				}
				
			});
		}

	}
}
