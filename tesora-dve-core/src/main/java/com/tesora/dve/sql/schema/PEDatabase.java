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
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaService;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.util.Pair;

public class PEDatabase extends Persistable<PEDatabase, UserDatabase> implements Database<PEAbstractTable<?>> {

	private SchemaEdge<PEPersistentGroup> defaultStorageGroup;
	private Name templateName;
	private TemplateMode templateMode;
	private final MultitenantMode mtmode;
	private FKMode fkmode;
	private String charSet;
	private String collation;
	
	// This enables late (on-site) name resolution.
	private boolean hasNameManglingEnabled = true;

	protected PESchema schema;
		
	@SuppressWarnings("unchecked")
	public PEDatabase(SchemaContext pc, Name n, PEPersistentGroup defStorage, Pair<Name, TemplateMode> templateDeclaration, MultitenantMode mtm, FKMode fkm,
			String charSet, String collation) {
		super(getDatabaseKey(n));
		setName(n);
		defaultStorageGroup = StructuralUtils.buildEdge(pc,defStorage,false); 
		schema = new PESchema(pc,this);
		templateName = templateDeclaration.getFirst();
		templateMode = templateDeclaration.getSecond();
		mtmode = mtm;
		setFKMode(fkm);
		this.charSet = charSet;
		this.collation = collation;
		setPersistent(pc,null,null);
	}

	// testing, transient schema, etc
	@SuppressWarnings("unchecked")
	public PEDatabase(SchemaContext pc, String n, MultitenantMode mtm, FKMode fkm) {
		this(n,new PESchema(pc), mtm, fkm);
		schema.pdb = StructuralUtils.buildEdge(pc,this,false);
	}
	
	protected PEDatabase(String n, PESchema givenSchema, MultitenantMode mtm, FKMode fkm) {
		super(getDatabaseKey(n));
		setName(new UnqualifiedName(n));
		defaultStorageGroup = null;
		schema = givenSchema;
		mtmode = mtm;
		setFKMode(fkm);
	}
	
	public static PEDatabase load(UserDatabase udb, SchemaContext pc) {
		PEDatabase e = (PEDatabase)pc.getLoaded(udb,getDatabaseKey(udb.getName()));
		if (e == null)
			e = new PEDatabase(udb, pc);
		return e;
	}
	
	@SuppressWarnings("unchecked")
	private PEDatabase(UserDatabase udb, SchemaContext pc) {
		super(getDatabaseKey(udb.getName()));
		pc.startLoading(this,udb);
		UnqualifiedName dbn = new UnqualifiedName(udb.getName());
		setPersistent(pc,udb, udb.getId());
		setName(dbn);
		setTemplateName(udb.getTemplateName());
		templateMode = TemplateMode.getModeFromName(udb.getTemplateMode());
		if (udb.getDefaultStorageGroup() != null) {
			defaultStorageGroup = StructuralUtils.buildEdge(pc,PEPersistentGroup.load(udb.getDefaultStorageGroup(), pc),true);
		}
		mtmode = udb.getMultitenantMode();
		fkmode = udb.getFKMode();
		schema = new PESchema(udb, pc, this);
		charSet = udb.getDefaultCharacterSetName();
		collation = udb.getDefaultCollationName();
		pc.finishedLoading(this, udb);
	}
	
	@Override
	public PEPersistentGroup getDefaultStorage(SchemaContext pc) { 
		return defaultStorageGroup.get(pc); 
	}
	
	// used in tests
	@Override
	public SchemaEdge<PEPersistentGroup> getDefaultStorageEdge() {
		return defaultStorageGroup;
	}
	
	@Override
	public PESchema getSchema() {	return schema; }

	public MultitenantMode getMTMode() { return mtmode; }

	public String getTemplateName() {
		return (this.templateName != null) ? this.templateName.get() : null;
	}
	
	public TemplateMode getTemplateMode() {
		return this.templateMode;
	}

	public void setTemplateName(final Name name) {
		this.templateName = name;
	}

	private void setTemplateName(final String name) {
		if ((name != null) && !name.isEmpty()) {
			this.templateName = new UnqualifiedName(name);
		}
	}

	public void setTemplateMode(final TemplateMode mode) {
		if (mode == null) {
			throw new NullPointerException("TEMPLATE MODE not specified");
		}
		this.templateMode = mode;
	}

	public boolean hasStrictTemplateMode() {
		return templateMode.isStrict();
	}

	public FKMode getFKMode() {
		return fkmode;
	}
	
	public void setFKMode(FKMode fkm) {
		if (fkm == FKMode.EMULATE)
			throw new SchemaException(Pass.SECOND, "No support for emulate fk mode");
		if (mtmode == MultitenantMode.ADAPTIVE && fkm != null && fkm != FKMode.STRICT)
			throw new SchemaException(Pass.SECOND, "Only fkmode=strict supported on adaptive multitenant database");
		fkmode = fkm;
	}
	
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEDatabase, UserDatabase> oth,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEDatabase other = oth.get();

		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);

		if (maybeBuildDiffMessage(sc, messages, "name", getName(), other.getName(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "template", getTemplateName(), other.getTemplateName(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "template_mode", getTemplateMode(), other.getTemplateMode(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "multitenant_mode", getMTMode(), other.getMTMode(), first, visited))
			return true;
		return false;
	}

	@Override
	protected String getDiffTag() {
		return "Database";
	}

	@Override
	public Persistable<PEDatabase, UserDatabase> reload(
			SchemaContext usingContext) {
		UserDatabase udb = usingContext.getCatalog().findUserDatabase(getName().getUnqualified().get());
		return PEDatabase.load(udb, usingContext);
	}
	
	public PEDatabase getOfDatabase() {
		return this;
	}
	public PEPersistentGroup getOfStorageGroup(SchemaContext pc) {
		if (defaultStorageGroup != null)
			return defaultStorageGroup.get(pc);
		return pc.getCurrentDatabase().getDefaultStorage(pc);
	}

	@Override
	protected int getID(UserDatabase p) {
		return p.getId();
	}

	@Override
	protected UserDatabase lookup(SchemaContext pc) throws PEException {
		return pc.getCatalog().findUserDatabase(name.getUnqualified().get());
	}

	@Override
	protected Persistable<PEDatabase, UserDatabase> load(SchemaContext pc, UserDatabase p)
			throws PEException {
		return new PEDatabase(p,pc);
	}

	@Override
	protected UserDatabase createEmptyNew(SchemaContext pc) throws PEException {
		PersistentGroup defsg = null;
		if (defaultStorageGroup != null && defaultStorageGroup.get(pc) != null)
			defsg = defaultStorageGroup.get(pc).persistTree(pc);
		UserDatabase udb = new UserDatabase(name.getUnqualified().get(), defsg, 
				this.getTemplateName(), templateMode,
				mtmode, 
				(fkmode == null ? FKMode.STRICT : fkmode), 
				charSet, collation);
		pc.getSaveContext().add(this,udb);
		return udb;
	}

	@Override
	protected void populateNew(SchemaContext pc, UserDatabase p) throws PEException {
		if (schema != null && schema.isDirty()) {
			schema.setDirty(pc,false);
			Collection<UserTable> ptabs = schema.persistTree(pc).values();
			for(UserTable ut : ptabs) {
				p.addUserTable(ut);
			}
		}
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return UserDatabase.class;
	}

	@Override
	public boolean isInfoSchema() {
		return false;
	}

	@Override
	public boolean hasNameManglingEnabled() {
		return this.hasNameManglingEnabled;
	}

	public void setNameMangling(final boolean enabled) {
		this.hasNameManglingEnabled = enabled;
	}

	@Override
	public String getNameOnSite(StorageSite site) {
		final String baseName = getName().get();
		if (this.hasNameManglingEnabled) {
			return UserDatabase.getNameOnSite(baseName, site);
		}

		return baseName;
	}

	@Override
	public String getUserVisibleName() {
		return getName().get();
	}

	@Override
	public int getId() {
		return getPersistentID().intValue();
	}

	public static SchemaCacheKey<Database<?>> getDatabaseKey(Name n) {
		return new DatabaseCacheKey(n.getUnquotedName().getUnqualified().get());
	}
	
	public static SchemaCacheKey<Database<?>> getDatabaseKey(String n) {
		return new DatabaseCacheKey(n);
	}
	
	public static class DatabaseCacheKey extends SchemaCacheKey<Database<?>> {

		private static final long serialVersionUID = 1L;
		private final String dbname;
		
		public DatabaseCacheKey(String dbn) {
			super();
			dbname = dbn;
		}
		
		@Override
		public int hashCode() {
			return initHash(PEDatabase.class, dbname.hashCode());
		}
		
		@Override
		public String toString() {
			return "PEDatabase:" + dbname;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof DatabaseCacheKey) {
				DatabaseCacheKey dlk = (DatabaseCacheKey) o;
				boolean match = dbname.equals(dlk.dbname);
				if (!match 
						&& PEConstants.INFORMATION_SCHEMA_DBNAME.equalsIgnoreCase(dbname) 
						&& PEConstants.INFORMATION_SCHEMA_DBNAME.equalsIgnoreCase(dlk.dbname))
						match = true;
				return match;
			}
			return false;
		}

		@Override
		public Database<?> load(SchemaContext sc) {
			String persistName = dbname;
			if (PEConstants.INFORMATION_SCHEMA_DBNAME.equals(persistName.toUpperCase(Locale.ENGLISH)))
				persistName = PEConstants.INFORMATION_SCHEMA_DBNAME;
			UserDatabase udb = sc.getCatalog().findUserDatabase(persistName);
			if (udb == null)
				return null;
            Database<?> peds = Singletons.require(InformationSchemaService.class).buildPEDatabase(sc, udb);
			if (peds == null) 
				peds = PEDatabase.load(udb, sc);
			return peds;
		}	
		
		@Override
		public Collection<SchemaCacheKey<?>> getCascades(Object o) {
			Database<?> d = (Database<?>) o;
			if (!d.isInfoSchema()) {
				PEDatabase db = (PEDatabase) d;
				return db.getSchema().getCascades();
			}
			return Collections.emptySet();
		}
	}

	public String getCharSet() {
		return charSet;
	}

	public void setCharSet(final String charSet) {
		if (charSet == null) {
			throw new NullPointerException("CHARACTER SET not specified");
		}
		this.charSet = charSet;
	}

	public String getCollation() {
		return collation;
	}

	public void setCollation(final String collation) {
		if (collation == null) {
			throw new NullPointerException("COLLATION not specified");
		}
		this.collation = collation;
	}

	@Override
	protected void updateExisting(SchemaContext sc, UserDatabase ud) throws PEException {
		super.updateExisting(sc, ud);

		ud.setDefaultCharacterSetName(this.charSet);
		ud.setDefaultCollationName(this.collation);
		ud.setTemplateName(this.getTemplateName());
		ud.setTemplateMode(this.templateMode);
	}

	@Override
	public String getDefaultCollationName() {
		return getCollation();
	}

	@Override
	public String getDefaultCharacterSetName() {
		return getCharSet();
	}
	
	public void setID(int pid) {
		persistentID = pid;
	}

}
