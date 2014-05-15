// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;

public class PEProject extends Persistable<PEProject, Project> {

	private PEPersistentGroup defaultStorageGroup;
	
	public PEProject(SchemaContext pc, Name n, PEPersistentGroup defStorage) {
		super(getProjectKey(n));
		setName(n);
		setPersistent(pc,null,null);
		defaultStorageGroup = defStorage;
	}
	
	public PEProject(SchemaContext pc, String n) {
		this(pc, new UnqualifiedName(n), null);
	}

	public static PEProject load(Project p, SchemaContext lc) {
		PEProject pep = (PEProject)lc.getLoaded(p, getProjectKey(p.getName()));
		if (pep == null)
			pep = new PEProject(p, lc);
		return pep;
	}
	
	private PEProject(Project p, SchemaContext lc) {
		super(getProjectKey(p.getName()));
		lc.startLoading(this,p);
		setName(new UnqualifiedName(p.getName()));
		if (p.getDefaultStorageGroup() != null)
			defaultStorageGroup = PEPersistentGroup.load(p.getDefaultStorageGroup(), lc);
		setPersistent(lc,p,p.getId());
		lc.finishedLoading(this, p);
	}
			
	public void setDefaultStorageGroup(PEPersistentGroup pesg) {
		defaultStorageGroup = pesg;
	}
	
	public PEPersistentGroup getDefaultStorageGroup() {
		return defaultStorageGroup;
	}
 	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEProject, Project> oth,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEProject other = oth.get();

		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);

		if (maybeBuildDiffMessage(sc, messages, "name", getName(), other.getName(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "default persistent group", getDefaultStorageGroup(), other.getDefaultStorageGroup(), first, visited))
			return true;
		return false;
	}

	@Override
	protected String getDiffTag() {
		return "Project";
	}

	@Override
	public Persistable<PEProject, Project> reload(
			SchemaContext usingContext) {
		return usingContext.findProject(getName());
	}

	@Override
	protected int getID(Project p) {
		return p.getId();
	}

	@Override
	protected Project lookup(SchemaContext pc) throws PEException {
		return pc.getCatalog().findProject(name.getUnqualified().get());
	}

	@Override
	protected Project createEmptyNew(SchemaContext pc) throws PEException {
		Project p = pc.getCatalog().createProject(name.getUnqualified().get());
		pc.getSaveContext().add(this,p);
		return p;
	}

	@Override
	protected Persistable<PEProject, Project> load(SchemaContext pc, Project p)
			throws PEException {
		return new PEProject(p,pc);
	}

	@Override
	protected void populateNew(SchemaContext pc, Project p) throws PEException {
		if (defaultStorageGroup != null) 
			p.setDefaultStorageGroup(defaultStorageGroup.persistTree(pc));
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return Project.class;
	}
	
	public static SchemaCacheKey<PEProject> getProjectKey(Name n) {
		return getProjectKey(n.getUnquotedName().get());
	}
	
	public static SchemaCacheKey<PEProject> getProjectKey(String n) {
		return new ProjectCacheKey(n);
	}
	
	public static class ProjectCacheKey extends SchemaCacheKey<PEProject> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final String name;
		
		public ProjectCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public int hashCode() {
			return initHash(PEProject.class,name.hashCode());
		}
		
		@Override
		public String toString() {
			return "PEProject:" + name;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof ProjectCacheKey) {
				ProjectCacheKey pk = (ProjectCacheKey) o;
				return name.equals(pk.name);
			}
			return false;
		}

		@Override
		public PEProject load(SchemaContext sc) {
			Project proj = sc.getCatalog().findProject(name);
			if (proj == null)
				return null;
			return PEProject.load(proj, sc);
		}
		
	}
}
