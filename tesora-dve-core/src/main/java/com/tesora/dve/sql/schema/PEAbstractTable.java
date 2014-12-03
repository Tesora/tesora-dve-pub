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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.mysql.MysqlEmitter;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.PEDatabase.DatabaseCacheKey;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.modifiers.CharsetTableModifier;
import com.tesora.dve.sql.schema.modifiers.CollationTableModifier;
import com.tesora.dve.sql.schema.modifiers.EngineTableModifier;
import com.tesora.dve.sql.schema.mt.TenantColumn;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.util.Cast;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

// everything that looks like a table derives from this
// userland tables
// userland views
// temp tables
public abstract class PEAbstractTable<T> extends Persistable<T, UserTable> implements Table<PEColumn> {

	// columns, in declaration order
	protected List<PEColumn> columns;
	// lookup helper
	protected SchemaLookup<PEColumn> lookup;
	// distribution vector
	protected DistributionVector dv;
	// persistent group
	protected SchemaEdge<PEStorageGroup> storage;
	// enclosing database
	protected SchemaEdge<PEDatabase> db;

	protected TableState state;

	private String createTableStatement;
	
	@SuppressWarnings("unchecked")
	protected PEAbstractTable(SchemaContext pc, Name name, 
			List<TableComponent<?>> fieldsAndKeys, DistributionVector dv,  
			PEPersistentGroup defStorage, PEDatabase db,
			TableState theState) {
		super(getTableKey(db,name));
		setName(name);
		this.storage = StructuralUtils.buildEdge(pc,defStorage,false);
		this.columns = new ArrayList<PEColumn>();
		initializeColumns(pc,fieldsAndKeys);
		setDatabase(pc,db,false);
		if (this.storage.get(pc) == null && this.db != null && this.db.has()) 
			this.storage = StructuralUtils.buildEdge(pc,this.db.get(pc).getDefaultStorage(pc),false);
		setDistributionVector(pc,dv,true);
		createTableStatement = null;
		state = theState;
        lookup = new SchemaLookup<PEColumn>(columns, false, false);
		setPersistent(pc,null,null);
	}
	
	// make a copy of other, consuming it
	protected PEAbstractTable(SchemaContext pc, PEAbstractTable other) {
		super(other.getCacheKey());
		setName(other.getName(pc,pc.getValues()));
		this.columns = new ArrayList<PEColumn>();
		this.storage = StructuralUtils.buildEdge(pc,other.getPersistentStorage(pc),false);
		ArrayList<TableComponent<?>> fieldsAndKeys = new ArrayList<TableComponent<?>>();
		fieldsAndKeys.addAll(other.getFields());
		fieldsAndKeys.addAll(other.getKeys());
		initializeColumns(pc,fieldsAndKeys);
		setDatabase(pc,other.getPEDatabase(pc),false);
		setDistributionVector(pc,other.getDistributionVector(pc),true);
		createTableStatement = other.createTableStatement;
		state = other.state;
		lookup = new SchemaLookup<PEColumn>(columns,false,false);
		setPersistent(pc,null,null);
	}
	
	public abstract PETable asTable();
	public abstract PEViewTable asView();

	protected abstract boolean isLoaded();
	protected abstract void setLoaded();
	
	public abstract String getTableType();
	
	public abstract boolean hasCardinalityInfo(SchemaContext sc);

	// for temp tables & virtual tables
	@SuppressWarnings("unchecked")
	protected PEAbstractTable(SchemaContext pc, Name name, List<PEColumn> cols, DistributionVector distVect, PEStorageGroup group, PEDatabase pdb) {
		super(null);
		setName(name);
		this.columns = new ArrayList<PEColumn>();
		for(PEColumn c : cols)
			addColumn(pc, c,true);
		setDistributionVector(pc,distVect);
		storage = StructuralUtils.buildEdge(pc,group, false);
		setDatabase(pc,pdb,false);
        lookup = new SchemaLookup<PEColumn>(columns, false, false);
		setPersistent(pc,null,null);
	}
	
	public static PEAbstractTable<?> load(UserTable table, SchemaContext lc) {
		PEAbstractTable<?> t = (PEAbstractTable<?>)lc.getLoaded(table,getTableKey(table));
		if (t == null) {
			if (table.getView() != null)
				t = new PEViewTable(table,lc);
			else
				t = new PETable(table, lc);
		}
		return t;
	}
	
	protected PEAbstractTable(UserTable table, SchemaContext lc) {
		super(getTableKey(table));
		
	}
	
	@SuppressWarnings("unchecked")
	protected void loadPersistent(UserTable table, SchemaContext lc) {
		UnqualifiedName tn = new UnqualifiedName(table.getName(),true);
		setName(tn);
		setPersistent(lc,table,table.getId());
		if (table.getPersistentGroup() != null)
			this.storage = StructuralUtils.buildEdge(lc,PEPersistentGroup.load(table.getPersistentGroup(), lc),true);
		setDatabase(lc,PEDatabase.load(table.getDatabase(), lc),true);
		try {
			this.createTableStatement = table.getCreateTableStmt();
		} catch (PEException pe) {
			this.createTableStatement = null;
		}
		state = table.getState();
		checkLoaded(lc);
	}
	
	protected void initializeColumns(SchemaContext pc, List<TableComponent<?>> fieldsAndKeys) {
		for(Iterator<TableComponent<?>> iter = fieldsAndKeys.iterator(); iter.hasNext();) {
			TableComponent<?> tc = iter.next();
			if (tc instanceof PEColumn) {
				PEColumn c = (PEColumn)tc;
				addColumn(pc, c,true);
				iter.remove();
			}
		}
	}
	
	protected List<TableComponent<?>> getKeys() {
		return Collections.EMPTY_LIST;
	}
	
	protected List<TableComponent<?>> getFields() {
		return Functional.apply(columns, new Cast<TableComponent<?>,PEColumn>());	
	}
	
	public TableState getState() {
		return state;
	}
	
	public void setState(TableState ts) {
		state = ts;
	}
	
	// one of our internal temp tables
	public boolean isTempTable() {
		return false;
	}

	public boolean isUserlandTemporaryTable(){
		return false;
	}
	
	public boolean isVirtualTable() {
		return false;
	}
	
	public boolean isView() {
		return false;
	}
	
	public void setFrozen() {
	}
	

	public boolean isTable() {
		return false;
	}
	
	public EngineTableModifier getEngine() {
		return null;
	}
	

	public CollationTableModifier getCollation() {
		return null;
	}

	public CharsetTableModifier getCharset() {
		return null;
	}
	

	
	protected PEColumn addColumn(SchemaContext pc, PEColumn c, boolean init) {
		if (!init)
			checkLoaded(pc);
		c.setPosition(this.columns.size());
		this.columns.add(c);
		if (this.lookup != null) this.lookup.refresh(this.columns);
		c.setTable(this);
		return c;
	}
	
	public void refreshColumnLookupTable() {
		if (this.lookup != null) {
			this.lookup.refresh(this.columns);
		}
	}

	@Override
	public PEColumn addColumn(SchemaContext pc, PEColumn c) {
		return addColumn(pc, c,false);
	}

	public void addColumns(SchemaContext pc, List<PEColumn> columns) {
		for (final PEColumn c : columns) {
			addColumn(pc, c, false);
		}
	}

	protected List<PEColumn> getColumns(SchemaContext pc, boolean init) {
		if (!init)
			checkLoaded(pc);
		return Collections.unmodifiableList(this.columns);
		
	}

	public List<PEKey> getKeys(SchemaContext sc) {
		return Collections.emptyList();
	}
	
	@Override
	public PEColumn lookup(SchemaContext pc, Name name) {
		return lookup.lookup(name);
	}
	
	// used in tests, and now in templates
	public PEColumn lookup(SchemaContext pc, String name) {
		checkLoaded(pc);
		return lookup(pc, new UnqualifiedName(name));
	}
	
	@Override
	public List<PEColumn> getColumns(SchemaContext pc) {
		return getColumns(pc,false);
	}

	public void removeColumn(SchemaContext pc, PEColumn c) {
		checkLoaded(pc);
		this.columns.remove(c);
	}
	
	public void removeColumns(SchemaContext pc, List<PEColumn> columns) {
		checkLoaded(pc);
		this.columns.removeAll(columns);
	}
	
	@Override
	public Name getName() { 
		return this.name;
	}
	
	@Override
	public Name getName(SchemaContext sc,ConnectionValues cv) {
		return getName();
	}
		
	@Override
	public boolean isInfoSchema() {
		return false;
	}

	protected void loadColumns(UserTable table, SchemaContext pc) {
		this.columns = new ArrayList<PEColumn>();
		for(UserColumn col : table.getUserColumns()) {
			PEColumn c = PEColumn.load(col, pc, null);
			addColumn(pc, c,true);
		}
		// set the lookup so that keys & dist vect may be resolved
        lookup = new SchemaLookup<PEColumn>(columns, false, false);
	}		

	protected void checkLoaded(SchemaContext pc) {
		if (isLoaded() || pc == null)
			return;
		UserTable table = getPersistent(pc,true);
		if (table == null)
			throw new IllegalStateException("missing persistent table");
		synchronized(this) {
			if (isLoaded())
				return;
			loadColumns(table,pc);
			dv = DistributionVector.load(pc, this, table);
			loadRest(table,pc);
			setLoaded();
		}
	}
	
	protected void loadRest(UserTable table, SchemaContext pc) {
	}
	
	public DistributionVector getDistributionVector(SchemaContext sc) {
		checkLoaded(sc);
		return this.dv; 
	}
	
	public void setDistributionVector(SchemaContext sc, DistributionVector dv) {
		setDistributionVector(sc,dv,false);
	}
	
	protected void setDistributionVector(SchemaContext sc, DistributionVector indv, boolean init) {
		if (indv instanceof UnresolvedRangeDistributionVector) {
			UnresolvedRangeDistributionVector unres = (UnresolvedRangeDistributionVector) indv;
			indv = unres.resolve(sc, getPersistentStorage(sc));
		}
		// if we have an existing distribution vector, go unset the dv offsets if applicable
		if (this.dv != null && this.dv.usesColumns(sc)) {
			for(PEColumn pec : this.dv.getColumns(sc)) {
				pec.setDistributionValuePosition(0);
			}	
		}
		this.dv = indv;
		if (this.dv != null) { 
			this.dv.setTable(sc, this);
			if (this.dv.usesColumns(sc)) {
				List<PEColumn> cols = this.dv.getColumns(sc,init);
				for(int i = 0; i < cols.size(); i++)
					cols.get(i).setDistributionValuePosition(i+1);
			}
		}
	}
	
	public boolean usesDV(SchemaContext sc) {
		checkLoaded(sc);
		if (dv == null)
			return false;
		return dv.usesColumns(sc);
	}

	public boolean isContainerBaseTable(SchemaContext sc) {
		return false;
	}
	
	public String getQualifiedPersistentName(SchemaContext sc) {
		return db.get(sc).getName().getUnquotedName().get() + "." + getName().getUnquotedName().get();
	}
	
	public PEPersistentGroup getPersistentStorage(SchemaContext pc) {
		if (this.storage.get(pc).isTempGroup()) return null;
		return (PEPersistentGroup)this.storage.get(pc);
	}
	@SuppressWarnings("unchecked")
	public void setStorage(SchemaContext pc, PEPersistentGroup peStorageGroup, boolean persistent) {
		this.storage = StructuralUtils.buildEdge(pc,peStorageGroup,persistent);
	}
	public PEStorageGroup getStorageGroup(SchemaContext sc) { return this.storage.get(sc); }
	public SchemaEdge<PEStorageGroup> getStorageGroupEdge() { return this.storage; }
	
	@SuppressWarnings("unchecked")
	public void setDatabase(SchemaContext sc, PEDatabase pdb, boolean persistent) {
		this.db = StructuralUtils.buildEdge(sc,pdb,persistent);
	}

	public TenantColumn getTenantColumn(SchemaContext sc) {
		return getTenantColumn(sc,false);
	}
	
	public TenantColumn getTenantColumn(SchemaContext sc, boolean init) {
		for(PEColumn c : getColumns(sc, init))
			if (c.isTenantColumn())
				return (TenantColumn) c;
		return null;
	}
	
	public ListSet<PEColumn> getDiscriminantColumns(SchemaContext sc) {
		ListSet<PEColumn> out = new ListSet<PEColumn>();
		for(PEColumn pec : getColumns(sc)) {
			if (pec.isPartOfContainerDistributionVector())
				out.add(pec);
		}
		return out;
	}
	
	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		return db.get(sc);
	}

	public boolean hasDatabase(SchemaContext sc) {
		if (db == null) {
			return false;
		}
		return (db.get(sc) != null);
	}
	
	public PEDatabase getPEDatabase(SchemaContext sc) {
		return (PEDatabase)getDatabase(sc);
	}
	
	// for temporary table support
	public Name getDatabaseName(SchemaContext sc) {
		return getDatabase(sc).getName();
	}

	// also for temp table support
	public MultitenantMode getEnclosingDatabaseMTMode(SchemaContext sc) {
		Database<?> db = getDatabase(sc);
		if (db instanceof PEDatabase) {
			PEDatabase pedb = (PEDatabase) db;
			return pedb.getMTMode();
		}
		return MultitenantMode.OFF;
	}

	// runtime statistics support
	public SchemaCacheKey<PEDatabase> getDatabaseCacheKey() {
		return db.getCacheKey();
	}
	
	public void setDeclaration(SchemaContext sc, PEAbstractTable<?> basedOn) {
		checkLoaded(sc);
		if (!getName().equals(basedOn.getName()))
			throw new SchemaException(Pass.PLANNER, "Invalid create table stmt: trying to set " + basedOn.getName() + " into table " + getName());
		createTableStatement = new MysqlEmitter().emitCreateTableStatement(sc, sc.getValues(),basedOn);
	}
	
	public String getDeclaration() {
		return createTableStatement;
	}
		
	@SuppressWarnings("unchecked")
	@Override
	public Persistable<T, UserTable> reload(SchemaContext usingContext) {
		UserTable ut = usingContext.getCatalog().findUserTable(getName(), getPEDatabase(usingContext).getPersistentID(), getDatabase(usingContext).getName().getUnquotedName().get());
		return (Persistable<T, UserTable>) PEAbstractTable.load(ut, usingContext);
	}
	
	@Override
	protected UserTable createEmptyNew(SchemaContext pc) throws PEException {
        String persistName = Singletons.require(HostService.class).getDBNative().getEmitter().getPersistentName(pc, pc.getValues(), this);
		UserDatabase pdb = this.db.get(pc).persistTree(pc);
		DistributionModel dm = dv.persistTree(pc);
		PersistentGroup sg = null;
		if (storage != null && storage.get(pc) != null)
			sg = storage.get(pc).persistTree(pc);
		else 
			sg = pdb.getDefaultStorageGroup();
		EngineTableModifier etm = getEngine();
		UserTable ut = new UserTable(persistName, dm, pdb, state, (etm == null ? null : etm.getPersistent()), getTableType());
		ut.setPersistentGroup(sg);
		pdb.addUserTable(ut);
		pc.getSaveContext().add(this,ut);
		return ut;
	}

	@Override
	protected void populateNew(SchemaContext pc,UserTable p) throws PEException {
		for(PEColumn col : getColumns(pc)) {
			p.addUserColumn(col.persistTree(pc));
		}
		if (db != null && db.get(pc) != null)
			p.setDatabase(this.db.get(pc).persistTree(pc));

		p.setCreateTableStmt(createTableStatement);
		if (dv != null) 
			dv.persistForTable(this, p, pc);
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return UserTable.class;
	}

	@Override
	public UserTable persistTree(SchemaContext pc, boolean forRefresh) throws PEException {
		if (!forRefresh)
			checkLoaded(pc);
		return super.persistTree(pc,forRefresh);
	}

	@Override
	protected void updateExisting(SchemaContext pc, UserTable ut) throws PEException {
		checkLoaded(pc);
		ut.setCreateTableStmt(createTableStatement);
		ut.setName(getName().get());
		ut.setState(state);
		updateExistingColumns(pc,ut);
	}
	
	private static UserColumn getUserColumnForStorage(final SchemaContext sc, final PEColumn source) throws PEException {
		final UserColumn nuc = source.persistTree(sc);
		nuc.setOrderInTable(source.getPosition() + 1);
		return nuc;
	}

	protected void updateExistingColumns(SchemaContext pc, UserTable ut) throws PEException {
		final LinkedHashMap<String, PEColumn> transCols = new LinkedHashMap<String, PEColumn>();
		// build a lookup table of names; key is transient name, value is persistent name.
		final ListOfPairs<String, String> transToPersMapping = new ListOfPairs<String, String>();
		for (final PEColumn c : columns) {
			final String transName = c.getName().getCapitalized().get();
			transCols.put(transName, c);
			if (c.getPersistentID() != null) {
				final String persName = c.getPersistent(pc).getName().toUpperCase().trim();
				if (!transName.equals(persName))
					transToPersMapping.add(transName, persName);
			}
		}

		pc.beginSaveContext();
		try {
			// build a lookup table for the persistent columns
			final LinkedHashMap<String, UserColumn> persCols = new LinkedHashMap<String, UserColumn>();
			for (final UserColumn uc : ut.getUserColumns()) {
				final String name = uc.getName().toUpperCase().trim();
				persCols.put(name, uc);
			}

			final List<UserColumn> userTableColumns = new ArrayList<UserColumn>(persCols.size());

			// do any renaming first
			for (final Pair<String, String> p : transToPersMapping) {
				final PEColumn pec = transCols.remove(p.getFirst());
				userTableColumns.add(getUserColumnForStorage(pc, pec));
			}
			

			for (final Map.Entry<String, UserColumn> pv : persCols.entrySet()) {
				final PEColumn any = transCols.remove(pv.getKey());
				final UserColumn uc = pv.getValue();
				if (any != null) {
					final PEColumn apc = PEColumn.load(uc, pc, null);
					final int ucPosition = uc.getOrderInTable(); // 1 based
					apc.setPosition(ucPosition - 1);
					final String anydiffs = any.differs(pc, apc, true);
					if (anydiffs != null) {
						// persist updated columns
						userTableColumns.add(getUserColumnForStorage(pc, any));
					} else {
						userTableColumns.add(uc); // reuse not updated
					}
				}
				
				// anything in persCols that wasn't found in transCols - deletion
				ut.removeUserColumn(uc);
			}
			
			// anything left in transCols that wasn't present in persCols - addition
			for (final PEColumn c : transCols.values()) {
				userTableColumns.add(getUserColumnForStorage(pc, c));
			}

			// order the columns by their position within the parent table
			Collections.sort(userTableColumns, new Comparator<UserColumn>() {
				@Override
				public int compare(UserColumn uc1, UserColumn uc2) {
					return uc1.getOrderInTable().compareTo(uc2.getOrderInTable());
				}
			});
			ut.addUserColumnList(userTableColumns);
			
		} finally {
			pc.endSaveContext();
		}
	}


	
	public static SchemaCacheKey<PEAbstractTable<?>> getTableKey(UserTable ut) {
		return new TableCacheKey(ut.getDatabase().getId(),ut.getDatabase().getName(),new UnqualifiedName(ut.getName()));
	}
	
	public static TableCacheKey getTableKey(PEDatabase within, Name tableName) {
		if (within == null) return null;
		int dbid = (within.getPersistentID() == null ? 0 : within.getPersistentID().intValue());
		return new TableCacheKey(dbid, within.getName().getUnquotedName().get(), tableName);
	}
	
	public static class TableCacheKey extends SchemaCacheKey<PEAbstractTable<?>> {

		private static final long serialVersionUID = 1L;
		private int dbid;
		private String dbName;
		private Name tabname;
		
		public TableCacheKey(int dbid, String dbn, Name tableName) {
			super();
			this.dbid = dbid;
			this.tabname = tableName.copy();
			this.tabname.prepareForSerialization();
			dbName = dbn;
		}
		
		@Override
		public int hashCode() {
			return addIntHash(addHash(initHash(PETable.class,tabname.getUnquotedName().getUnqualified().get().hashCode()),dbName.hashCode()),dbid);
		}

		@Override
		public String toString() {
			return "PETable:" + dbid + "(" + dbName + ")/" + tabname.getUnquotedName().getUnqualified().get();
		}
		
		public String getDatabaseName() {
			return dbName;
		}
		
		public Name getTableName() {
			return tabname;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof TableCacheKey) {
				TableCacheKey petk = (TableCacheKey) o;
				return this.dbid == petk.dbid && this.tabname.get().equals(petk.tabname.get());
			}
			return false;
		}

		@Override
		public PEAbstractTable<?> load(SchemaContext sc) {
			UserTable ut = sc.getCatalog().findUserTable(tabname, dbid, dbName);
			if (ut == null)
				return null;
			return PEAbstractTable.load(ut, sc);
		}

		@Override
		public CacheSegment getCacheSegment() {
			return CacheSegment.TABLE;
		}

		@Override
		public Collection<SchemaCacheKey<?>> getCascades(Object obj) {
			PEAbstractTable<?> pet = (PEAbstractTable<?>) obj;
			if (pet.isTable()) {
				PETable t = (PETable)pet;
				if (t.getReferencingTables() == null) return Collections.emptyList();
				return Functional.apply(t.getReferencingTables(), new UnaryFunction<SchemaCacheKey<?>,SchemaCacheKey<PEAbstractTable<?>>>() {

					@Override
					public SchemaCacheKey<?> evaluate(SchemaCacheKey<PEAbstractTable<?>> object) {
						return object;
					}
					
				});
			}
			return super.getCascades(obj);
		}

		@Override
		public LockSpecification getLockSpecification(String reason) {
			return new TableUseLock(reason, dbName,tabname.getUnquotedName().get());
		}
		
	}

	@Override
	protected UserTable lookup(SchemaContext pc) throws PEException {
		if (persistentID == null) return null;
		PEDatabase peds = this.db.get(pc);
		return pc.getCatalog().findUserTable(this.getName(), peds.getPersistentID(), peds.getName().getUnquotedName().get());
	}

	@Override
	protected int getID(UserTable p) {
		return p.getId();
	}

	public PEAbstractTable<?> recreate(SchemaContext sc, String decl, LockInfo li) {
		Database<?> cdb = sc.getCurrentDatabase(false);
		ParserOptions prevOptions = sc.getOptions();
		if (prevOptions == null)
			prevOptions = ParserOptions.NONE;
		ParserOptions subOptions = prevOptions
				.setAllowDuplicates()
				.setAllowTenantColumn()
				.setTSchema()
				.setOmitMetadataInjection()
				.disableMTLookupChecks()
				.setOmitTenantColumnInjection();
		if (li != null)
			subOptions = subOptions.setLockOverride(li);
		PETable tschemaVersion = null;
		ValueManager vm = sc.getValueManager();
		ConnectionValues cv = sc.getValues();
		sc.setValueManager(new ValueManager());
		try {
			sc.setCurrentDatabase(getDatabase(sc));
			List<Statement> prereq = InvokeParser.parse(InvokeParser.buildInputState(decl,sc), subOptions, sc).getStatements();
			for(Statement s : prereq) {
				if (s instanceof PECreateStatement) {
					PECreateStatement<?,?> pecs = (PECreateStatement<?,?>) s;
					if (pecs.getRoot() instanceof PETable) {
						tschemaVersion = (PETable) pecs.getRoot();
						break;
					}
				}
			}
		} finally {
			sc.setOptions(prevOptions);
			sc.setCurrentDatabase(cdb);
			sc.setValueManager(vm);
			sc.setValues(cv);
		}
		if (tschemaVersion == null) {
			throw new SchemaException(Pass.PLANNER, "Unable to generate create table statement");
		}
		if (!tschemaVersion.getName().equals(getName()))
			throw new SchemaException(Pass.PLANNER, "Wrong recreated tschema table.  Have " + getName() + " but recreated " + tschemaVersion.getName());

		// make sure we keep the original dist vect - we might have applied a new one if the template has changed
		tschemaVersion.dv = dv.adapt(sc, tschemaVersion);
		
		return tschemaVersion;
	}

}
