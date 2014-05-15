// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;
import java.util.Set;

import com.tesora.dve.common.SiteInstanceStatus;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.UserAuthentication;

public class PESiteInstance extends Persistable<PESiteInstance, SiteInstance> {
	static final String DIFF_TAG = "Persistent instance"; 
			
	public static final String OPTION_URL = "url";
	public static final String OPTION_USER = "user";
	public static final String OPTION_PASSWORD = "password";
	public static final String OPTION_MASTER = "master";
	public static final String OPTION_STATUS = "status";
	
	String url;
	String user;
	String password;
	Boolean master = false;
	String status = SiteInstanceStatus.ONLINE.name();
	List<Pair<Name, LiteralExpression>> options;

	public PESiteInstance(SchemaContext pc, SiteInstance si) {
		super(getSiteInstanceKey(si.getName()));
		pc.startLoading(this, si);
		
		UnqualifiedName sn = new UnqualifiedName(si.getName());
		setName(sn);
		
		this.url = si.getInstanceURL();
		this.user = si.getUser();
		this.password = si.getDecryptedPassword();
		this.master = si.isMaster();
		this.status = si.getStatus();
		
		setPersistent(pc,si, si.getId());
		pc.finishedLoading(this, si);
	}

	public PESiteInstance(SchemaContext pc, Name siteInstanceName) {
		super(getSiteInstanceKey(siteInstanceName));
		
		setName(siteInstanceName);
		setPersistent(pc,null,null);		
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return SiteInstance.class;
	}

	@Override
	protected SiteInstance lookup(SchemaContext pc) throws PEException {
		SiteInstance si = pc.getCatalog().findSiteInstance(name.getUnqualified().get());
		return si;
	}

	@Override
	protected SiteInstance createEmptyNew(SchemaContext pc) throws PEException {
		SiteInstance ss = pc.getCatalog().createSiteInstance(name.getUnqualified().get(), 
				url, user, password, master, status);
		pc.getSaveContext().add(this,ss);
		return ss;
	}
	
	@Override
	protected void populateNew(SchemaContext pc, SiteInstance p) throws PEException {
	}

	@Override
	protected Persistable<PESiteInstance, SiteInstance> load(SchemaContext pc, SiteInstance p)
			throws PEException {
		return PESiteInstance.load(p, pc);
	}

	@Override
	protected int getID(SiteInstance p) {
		return p.getId();
	}

	@Override
	public Persistable<PESiteInstance, SiteInstance> reload(
			SchemaContext usingContext) throws PEException {
		return usingContext.findSiteInstance(getName());
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages,
			Persistable<PESiteInstance, SiteInstance> other, boolean first,
			@SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PESiteInstance opes = other.get();

		if (visited.contains(this) && visited.contains(opes)) {
			return false;
		}
		visited.add(this);
		visited.add(opes);
		
		if (maybeBuildDiffMessage(sc,messages,"name",name.getSQL(), other.getName().getSQL(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc,messages,"jdbc " + OPTION_URL, getUrl(), opes.getUrl(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc,messages,OPTION_USER, getUser(), opes.getUser(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc,messages,OPTION_PASSWORD, getPassword(), opes.getPassword(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc,messages,OPTION_MASTER, getMaster(), opes.getMaster(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc,messages,OPTION_STATUS, getStatus(), opes.getStatus(), first, visited))
			return true;
		return false;
	}

	@Override
	protected String getDiffTag() {
		return DIFF_TAG;
	}

	public static PESiteInstance load(SiteInstance si, SchemaContext context) {
		PESiteInstance pesg = (PESiteInstance)context.getLoaded(si,getSiteInstanceKey(si.getName()));
		if (pesg == null)
			pesg = new PESiteInstance(context, si);
		return pesg;
	}
	
	@Override
	protected void update(SchemaContext pc, SiteInstance p) throws PEException {
		p.setStatus(getStatus());
		p.setMaster(getMaster());
		p.setInstanceURL(getUrl());
		p.setUser(getUser());
		p.setDecryptedPassword(getPassword());
		super.update(pc,p);
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String u) {
		url = u;
	}
	
	public String getUser() {
		return user;
	}
	
	public void setUser(String u) {
		user = u;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String p) {
		password = p;
	}
	
	public UserAuthentication getAuthentication() {
		return new UserAuthentication(user, password, false);
	}
	
	public Boolean getMaster() {
		return master;
	}

	public String getStatus() {
		return status;
	}
	
	public void setStatus(String v) {
		status = v;
	}
	
	public static SchemaCacheKey<PESiteInstance> getSiteInstanceKey(Name name) {
		return getSiteInstanceKey(name.getUnquotedName().get());
	}
	
	public static SchemaCacheKey<PESiteInstance> getSiteInstanceKey(String n) {
		return new SiteInstanceCacheKey(n);
	}
	
	public static class SiteInstanceCacheKey extends SchemaCacheKey<PESiteInstance> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final String name;
		
		public SiteInstanceCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public int hashCode() {
			return initHash(PESiteInstance.class, name.hashCode());
		}
		
		@Override
		public String toString() {
			return "PESiteInstance:" + name;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof SiteInstanceCacheKey) {
				SiteInstanceCacheKey silk = (SiteInstanceCacheKey) o;
				return name.equals(silk.name);
			}
			return false;
		}

		@Override
		public PESiteInstance load(SchemaContext sc) {
			SiteInstance si = sc.getCatalog().findSiteInstance(name);
			if (si == null) {
				return null;
			}
			return PESiteInstance.load(si, sc);
		}
	}
		
	private TCacheSiteInstance tcache = null;
	
	public ISiteInstance getTCache() {
		if (tcache == null) tcache = new TCacheSiteInstance(this);
		return tcache;
	}
	
	public static class TCacheSiteInstance implements ISiteInstance {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private PESiteInstance si;
		
		public TCacheSiteInstance(PESiteInstance pesi) {
			si = pesi;
		}

		@Override
		public int getId() {
			return si.getPersistentID();
		}

		@Override
		public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
				throws PEException {
			return null;
		}

		@Override
		public ResultRow getShowResultRow(CatalogQueryOptions cqo)
				throws PEException {
			return null;
		}

		@Override
		public void removeFromParent() throws Throwable {
		}

		@Override
		public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
				throws Throwable {
			return null;
		}

		@Override
		public void onUpdate() {
		}

		@Override
		public void onDrop() {
		}

	}
}
