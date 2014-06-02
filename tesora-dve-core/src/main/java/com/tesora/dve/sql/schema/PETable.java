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


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.tesora.dve.queryplan.TempTableGenerator;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.codec.binary.Hex;

import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.common.catalog.PersistentContainer;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.modifiers.AutoincTableModifier;
import com.tesora.dve.sql.schema.modifiers.CharsetTableModifier;
import com.tesora.dve.sql.schema.modifiers.CollationTableModifier;
import com.tesora.dve.sql.schema.modifiers.CommentTableModifier;
import com.tesora.dve.sql.schema.modifiers.CreateOptionModifier;
import com.tesora.dve.sql.schema.modifiers.EngineTableModifier;
import com.tesora.dve.sql.schema.modifiers.EngineTableModifier.EngineTag;
import com.tesora.dve.sql.schema.modifiers.RowFormatTableModifier;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.schema.modifiers.TableModifierTag;
import com.tesora.dve.sql.schema.modifiers.TableModifiers;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryPredicate;

public class PETable extends PEAbstractTable<PETable> implements HasComment { 
		
	private List<PEKey> keys;
	// duplicated for convenience
	private PEKey pk;
	
	private Boolean hasCardInfo = null;
	
	// tables which have fks which refer to this table.  used in fk action support.
	private ListSet<SchemaCacheKey<PEAbstractTable<?>>> referring;
	
	// table options - this encompasses both those persisted separately and those not.
	// for non-new tables (i.e. loaded) this contains the options separately persisted.
	TableModifiers modifiers;

	private String tableDefinition;
	
	// cache this for the truncate case
	private Boolean hasAutoInc = null;
	// keep track of our autoinc tracker id to avoid a catalog lookup
	private Integer autoIncTrackerID;

	private boolean loaded;
	
	// cache object; used to avoid some catalog access issues in the engine
	// also used for cta support
	protected CachedPETable cached;
	
	public PETable(SchemaContext pc, Name name, 
			List<TableComponent<?>> fieldsAndKeys, DistributionVector dv, List<TableModifier> modifier, 
			PEPersistentGroup defStorage, PEDatabase db,
			TableState theState) {
		super(pc,name,fieldsAndKeys,dv,defStorage,db,theState);
		loaded = true;
		this.pk = null;
		this.referring = new ListSet<SchemaCacheKey<PEAbstractTable<?>>>();
		this.keys = new ArrayList<PEKey>();
		this.modifiers = new TableModifiers(modifier);
		// do keys & columns first so that database can propagate charset/collation
		initializeColumnsAndKeys(pc,fieldsAndKeys,db);
		setDatabase(pc,db,false);
		autoIncTrackerID = null;
		forceStorage(pc);
		setPersistent(pc,null,null);
		cached = null;
	}
		
	@Override
	public boolean isTable() {
		return true;
	}
	
	@Override
	public PETable asTable() {
		return this;
	}
	
	@Override
	public PEViewTable asView() {
		throw new IllegalStateException("Cannot cast a table to a view");
	}
	
	@Override
	protected boolean isLoaded() {
		return loaded;
	}
	
	@Override
	protected void setLoaded() {
		loaded = true;
	}
	
	@SuppressWarnings("unchecked")
	private void initializeColumnsAndKeys(SchemaContext pc, List<TableComponent<?>> fieldsAndKeys, PEDatabase db) {
		// add in canonical order:  columns, nonfks, fks
		for(PEColumn p : getColumns(pc,true)) {
			if (p.isAutoIncrement())
				hasAutoInc = Boolean.TRUE;
		}
		for(Iterator<TableComponent<?>> iter = fieldsAndKeys.iterator(); iter.hasNext();) {
			PEKey ktc = (PEKey) iter.next();
			if (ktc.getConstraint() == ConstraintType.FOREIGN)
				continue;
			addKey(pc, ktc,false,db);
			if (ktc.isPrimary()) {
				if (this.pk != null)
					throw new SchemaException(Pass.SECOND,"Only one primary key per table, please");
				this.pk = ktc;
			}
			ktc.setTable(StructuralUtils.buildEdge(pc,this,false));
			iter.remove();
		}
		for(Iterator<TableComponent<?>> iter = fieldsAndKeys.iterator(); iter.hasNext();) {
			PEForeignKey pefk = (PEForeignKey) iter.next();
			addKey(pc,pefk,false,db);
		}
	}
	
	// placeholder table ctor
	@SuppressWarnings("unchecked")
	public PETable(SchemaContext pc, Name name, List<PEColumn> cols, DistributionVector distVect, PEStorageGroup group, PEDatabase pdb) {
		super(pc,name,cols,distVect,group,pdb);
		loaded = true;
		this.pk = null;
		this.keys = new ArrayList<PEKey>();
		autoIncTrackerID = null;
		modifiers = new TableModifiers();
		setDatabase(pc,pdb,false);
		forceStorage(pc);
		setPersistent(pc,null,null);
		cached = null;
	}
	
	@SuppressWarnings("unchecked")
	protected PETable(UserTable table, SchemaContext lc) {
		super(table,lc);
		loaded = false;
		lc.startLoading(this, table);
		modifiers = new TableModifiers();
		modifiers.setModifier(new EngineTableModifier(EngineTableModifier.EngineTag.findEngine(table.getEngine())));
		if (table.getCollation() != null)
			modifiers.setModifier(new CollationTableModifier(new UnqualifiedName(table.getCollation())));
		if (table.getComment() != null)
			modifiers.setModifier(new CommentTableModifier(new Comment(table.getComment())));
		if (table.getRowFormat() != null)
			modifiers.setModifier(new RowFormatTableModifier(new UnqualifiedName(table.getRowFormat())));
		loadPersistent(table,lc);
		setDatabase(lc,PEDatabase.load(table.getDatabase(), lc),true);
		this.tableDefinition = (table.getShape() == null ? null : table.getShape().getTableDefinition()); 
		if (table.hasAutoIncr())
			autoIncTrackerID = table.getAutoIncr().getId();
		checkLoaded(lc);
		cached = new CachedPETable(lc, this);
		lc.finishedLoading(this, table); 
	}
	
	@SuppressWarnings("unchecked")
	protected void loadRest(UserTable table, SchemaContext pc) {
		for(PEColumn c : getColumns(pc,true)) {
			if (c.isAutoIncrement())
				hasAutoInc = Boolean.TRUE;
		}
		this.keys = new ArrayList<PEKey>();
		for(Key k : table.getKeys()) {
			PEKey pek = PEKey.load(k, pc, this);
			if (pek.isPrimary())
				pk = pek;
			keys.add(pek);
			pek.setTable(StructuralUtils.buildEdge(pc,this, true));
		}
		referring = new ListSet<SchemaCacheKey<PEAbstractTable<?>>>();
		for(Key k : table.getReferringKeys()) {
			referring.add(PEAbstractTable.getTableKey(k.getTable()));
		}
		forceStorage(pc);
	}		
		
	public void setDeclaration(SchemaContext sc, PETable basedOn) {
		super.setDeclaration(sc,basedOn);
        tableDefinition = Singletons.require(HostService.class).getDBNative().getEmitter().emitTableDefinition(sc,basedOn);
	}
	
	public String getDefinition() {
		return tableDefinition;
	}
	
	public String getTypeHash() {
		return buildTypeHash(tableDefinition);
	}
	
	public static String buildTypeHash(String in) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			byte[] bout = md.digest(PECharsetUtils.getBytes(in, PECharsetUtils.UTF_8));
			String out = new String(Hex.encodeHex(bout, true));
			return out;
		} catch (NoSuchAlgorithmException nsae) {
			throw new SchemaException(Pass.PLANNER, "Unable to obtain sha-1 hash of type definition");
		}
	}
	
	// used in mt mode
	public void resetName(SchemaContext sc, UnqualifiedName unq) {
		setName(unq);
		int counter = 0;
		for(PEKey pek : getKeys(sc)) {
			if (!pek.isForeign()) continue;
			PEForeignKey pefk = (PEForeignKey) pek;
			pefk.setPhysicalSymbol(new UnqualifiedName(getName().getUnqualified().getUnquotedName().get() + "_ibfk_" + (++counter)));
		}
	}
		
	@SuppressWarnings("unchecked")
	public void setDatabase(SchemaContext sc, PEDatabase pdb, boolean persistent) {
		this.db = StructuralUtils.buildEdge(sc,pdb,persistent);
		if (pdb != null && modifiers != null) {
			CharsetTableModifier charset = (CharsetTableModifier) modifiers.getModifier(TableModifierTag.DEFAULT_CHARSET); 
			CollationTableModifier collation = (CollationTableModifier) modifiers.getModifier(TableModifierTag.DEFAULT_COLLATION); 
			if (charset == null) {
				charset = new CharsetTableModifier(new UnqualifiedName(pdb.getCharSet()));
				modifiers.setModifier(charset);
			}
			if (collation == null) {
				collation = new CollationTableModifier(new UnqualifiedName(pdb.getCollation()));
				modifiers.setModifier(collation);
			}
			for(PEColumn pec : getColumns(sc)) {
				pec.takeCharsetSettings(charset, collation, false);
			}
		}
	}
	
	public boolean isContainerBaseTable(SchemaContext sc) {
		checkLoaded(sc);
		if (dv == null) return false;
		if (!dv.isContainer()) return false;
		return dv.getContainer(sc).getBaseTable(sc) == this;
	}
		
	@Override
	public void setComment(Comment c) {
		if (modifiers == null) 
			modifiers = new TableModifiers();
		modifiers.setModifier(new CommentTableModifier(c));
	}
	
	@Override
	public Comment getComment() {
		CommentTableModifier ctm = (CommentTableModifier) modifiers.getModifier(TableModifierTag.COMMENT);
		if (ctm == null) return null;
		return ctm.getComment();
	}

	public TableModifiers getModifiers() {
		return modifiers;
	}
	
	public Long getAutoIncOffset(SchemaContext sc) {
		AutoincTableModifier atm = (AutoincTableModifier) modifiers.getModifier(TableModifierTag.AUTOINCREMENT);
		if (atm == null) return null;
		return atm.getStartAt();
	}
	
	public EngineTableModifier getEngine() {
		return (EngineTableModifier) modifiers.getModifier(TableModifierTag.ENGINE);
	}
	
	public CollationTableModifier getCollation() {
		return (CollationTableModifier) modifiers.getModifier(TableModifierTag.DEFAULT_COLLATION);
	}

	public CharsetTableModifier getCharset() {
		return (CharsetTableModifier) modifiers.getModifier(TableModifierTag.DEFAULT_CHARSET);
	}
	
	public boolean shouldEmitCharset() {
		return true;
	}
	
	public boolean shouldEmitCollation(SchemaContext sc) {
		CollationTableModifier ctm = getCollation();
		if (ctm == null) return false;
		PEDatabase ofdb = getPEDatabase(sc);
		if (ofdb == null) return false;
		return !ctm.getCollation().getUnquotedName().get().equals(ofdb.getCollation());
	}
	
	public List<PEKey> getKeys(SchemaContext sc) {
		checkLoaded(sc);
		return keys;
	}

	
	public PEKey lookupKey(SchemaContext sc, Name keyName) {
		if (!loaded) return null;
		PEKey asConstraint = null;
		for(PEKey pek : getKeys(sc)) {
			if (pek.getName().equals(keyName))
				return pek;
			else if (pek.getSymbol() != null && pek.getSymbol().equals(keyName))
				asConstraint = pek;
		}
		return asConstraint;
	}
	
	public PEKey addKey(SchemaContext sc, PEKey pek, boolean doSyntheticChecks) {
		return addKey(sc, pek, doSyntheticChecks, getPEDatabase(sc));
	}
	
	@SuppressWarnings("unchecked")
	public PEKey addKey(SchemaContext sc, PEKey pek, boolean doSyntheticChecks, PEDatabase theDB) {
		checkLoaded(sc);
		PEKey newlyDropped = null;
		boolean mtmode = theDB.getMTMode().isMT();
		if (pek.getConstraint() == ConstraintType.FOREIGN) {
			if (!mtmode) {
				PEForeignKey pefk = (PEForeignKey) pek;
				PEKey exists = pefk.findPrefixKey(sc, this);
				if (exists == null) {
					exists = pefk.buildPrefixKey(sc,this);
					addKey(sc,exists,false,theDB);
				}
			} 
		} else if (!pek.isSynthetic() && doSyntheticChecks && !mtmode) {
			for(PEKey cpek : keys) {
				if (!cpek.isSynthetic()) continue;
				if (PEKey.samePrefix(cpek, pek)) {
					newlyDropped = cpek;
				}
			}
		}
		generateNames(sc, pek, mtmode);
		// canonical order for keys is:
		// any primary key
		// all unique keys
		// other keys
		// foreign keys

		if (keys.isEmpty()) {
			keys.add(pek);
		} else if (pek.getConstraint() == ConstraintType.PRIMARY) {
			keys.add(0, pek);
		} else if (pek.getConstraint() == ConstraintType.FOREIGN) {
			keys.add(pek);
		} else {
			PEKey thePK = null;
			List<PEKey> uniques = new ArrayList<PEKey>();
			List<PEKey> regulars = new ArrayList<PEKey>();
			List<PEKey> foreigns = new ArrayList<PEKey>();
			for(PEKey ipek : keys) {
				if (ipek.getConstraint() == ConstraintType.PRIMARY)
					thePK = ipek;
				else if (ipek.getConstraint() == ConstraintType.UNIQUE)
					uniques.add(ipek);
				else if (ipek.getConstraint() == ConstraintType.FOREIGN)
					foreigns.add(ipek);
				else
					regulars.add(ipek);
			}
			if (pek.getConstraint() == ConstraintType.UNIQUE)
				uniques.add(pek);
			else
				regulars.add(pek);
			keys.clear();
			if (thePK != null)
				keys.add(thePK);
			keys.addAll(uniques);
			keys.addAll(regulars);
			keys.addAll(foreigns);
		}		
		pek.setTable(StructuralUtils.buildEdge(sc,this, false));
		return newlyDropped;
	}

	public void generateNames(SchemaContext sc, PEKey pek, boolean mtMode) {
		generateKeyName(sc, pek);
		if (pek.getConstraint() == null) {
		} else if (pek.getSymbol() != null) {
		} else if (pek.getConstraint() == ConstraintType.PRIMARY) {
			pek.setSymbol(new UnqualifiedName("PRIMARY"));
		} else if (pek.getConstraint() == ConstraintType.UNIQUE) {
			pek.setSymbol(pek.getName());
		} else if (pek.getConstraint() == ConstraintType.FOREIGN) {
			pek.setSymbol(buildFKSymbol(sc));
		}
		if (pek.getConstraint() == ConstraintType.FOREIGN && mtMode) {
			PEForeignKey pefk = (PEForeignKey) pek;
			pefk.setPhysicalSymbol(buildFKSymbol(sc));
		}
	}
	
	private void generateKeyName(SchemaContext sc, PEKey pek) {
		if (pek.getName() == null) {
			if (pek.isPrimary()) {
				pek.setName(new UnqualifiedName("PRIMARY"));
				pek.setSymbol(new UnqualifiedName("PRIMARY"));
			} else if (!pek.isForeign()) {
				Name candidate = pek.getKeyColumns().get(0).getColumn().getName();
				Name nn = null;
				int counter = 0;
				while(nn == null) {
					nn = (counter == 0 ? candidate : new UnqualifiedName(candidate.getUnqualified().getUnquotedName().get() + "_" + counter,true));
					for(PEKey opek : keys) {
						if (opek.getName().getUnquotedName().equals(nn.getUnquotedName())) {
							nn = null;
							counter++;
							break;
						}
					}
				}
				pek.setName(nn);
			} else {
				if (pek.getSymbol() != null) {
					pek.setName(pek.getSymbol());
				} else {
					Name symbol = buildFKSymbol(sc);
					pek.setName(symbol);
					pek.setSymbol(symbol);
				}
				if (isDuplicateFKSymbol(sc, pek.getSymbol().get()))
					throw new SchemaException(Pass.SECOND, "Duplicate foreign key name: " + pek.getSymbol().get());
				
			}
		} else {
			if (pek.getConstraint() == ConstraintType.FOREIGN && pek.getSymbol() != null) {
				if (isDuplicateFKSymbol(sc, pek.getSymbol().get()))
					throw new SchemaException(Pass.SECOND, "Duplicate foreign key name: " + pek.getSymbol().get());
				
				for(PEKey p : getKeys(sc)) {
					if (p.getConstraint() != ConstraintType.FOREIGN) {
						continue;
					}
					if (p.getSymbol().equals(pek.getSymbol()))
						throw new SchemaException(Pass.SECOND, "Duplicate foreign key name: " + pek.getSymbol().get());
				}
			} else {
				for(PEKey p : getKeys(sc)) {
					if (p.getConstraint() == ConstraintType.FOREIGN) {
						continue;
					}
					if (p.getName().equals(pek.getName()))
						throw new SchemaException(Pass.SECOND, "Duplicate key name: " + pek.getName());
				}
			}
		}
	}

	private UnqualifiedName buildFKSymbol(SchemaContext sc) {
		// foreign keys follow tablename_ibfk_n; the _n is the number of fks
		int n = getForeignKeys(sc).size() + 1;
		return new UnqualifiedName(getName().getUnqualified().getUnquotedName().get() + "_ibfk_" + n);
	}

	private boolean isDuplicateFKSymbol(SchemaContext sc, String symbol) {
		boolean ret = false;
		
		try {
			Long tenid = sc.getPolicyContext().getTenantID(false);
			Integer tenantID = (tenid == null ? null : tenid.intValue());
			PEKey fKey = sc.findForeignKey(hasDatabase(sc) ? getDatabase(sc) : null, tenantID, null, symbol);
			if (fKey != null) {
				PETable t = fKey.getTable(sc);
				if (t != null && !t.equals(this))
					ret = true;
			}
		} catch(Exception e) {
			ret = false;
		}
		return ret;
	}
	
	public void removeKey(SchemaContext sc, PEKey pek) {
		checkLoaded(sc);
		keys.remove(pek);
	}
	
	public int getOffsetOf(SchemaContext sc, PEKey pek) {
		for(int i = 0; i < keys.size(); i++) {
			PEKey me = keys.get(i);
			if (me.getConstraint() == pek.getConstraint() && me.getName().equals(pek.getName()))
				return i+1;
		}
		throw new SchemaException(Pass.PLANNER, "Cannot find key " + pek + " within table " + getName());
	}
	
	public List<PEForeignKey> getForeignKeys(SchemaContext sc) {
		List<PEKey> all = getKeys(sc);
		List<PEForeignKey> out = new ArrayList<PEForeignKey>();
		for(PEKey pek : all)
			if (pek.isForeign())
				out.add((PEForeignKey) pek);
		return out;
	}

	public void removeForeignKeys(SchemaContext sc) {
		for(PEForeignKey fk : getForeignKeys(sc)) {
			removeKey(sc, fk);
		}
	}
	
	public List<PEKey> getUniqueKeys(SchemaContext sc) {
		List<PEKey> all = getKeys(sc);
		return Functional.select(all, new UnaryPredicate<PEKey>() {

			@Override
			public boolean test(PEKey object) {
				if (object.isForeign()) return false;
				if (!object.isUnique()) return false;
				return true;
			}
			
		});
	}
	
	public PEKey getPrimaryKey(SchemaContext sc) {
		checkLoaded(sc);
		return pk;
	}
	
	public PEKey getUniqueKey(SchemaContext sc) {
		checkLoaded(sc);
		if (pk != null) return pk;
		for(PEKey pek : getKeys(sc)) {
			if (pek.isUnique())
				return pek;
		}
		return null;
	}
	
	public boolean isPrimaryKeyPart(SchemaContext sc, PEColumn c) {
		checkLoaded(sc);
		if (pk == null) return false;
		return pk.containsColumn(c);
	}
	
	public ListSet<SchemaCacheKey<PEAbstractTable<?>>> getReferencingTables() {
		return referring;
	}
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PETable, UserTable> oth,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PETable other = oth.get();

		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);

		if (maybeBuildDiffMessage(sc,messages, "name", getName(), other.getName(), first, visited))
			return true;
		if ((dv == null ? 1 : 0) != (other.getDistributionVector(sc) == null ? 1 : 0)) {
			if (dv == null)
				messages.add("Extra distribution vector present");
			else
				messages.add("Distribution vector missing.");
			if (first)
				return true;
		} else if (dv != null && other != null &&
				dv.collectDifferences(sc, messages, other.getDistributionVector(sc), first, visited))
			return true;
		if (compareColumns(sc,messages,other,first,visited))
			return true;
		if (compareKeys(sc,messages,other,first,visited))
			return true;
		return false;
	}

	private boolean compareColumns(SchemaContext sc, List<String> messages, PETable other,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		if (maybeBuildDiffMessage(sc,messages, "number of columns", getColumns(sc).size(), other.getColumns(sc).size(), first, visited))
			return true;
		Iterator<PEColumn> leftIter = getColumns(sc).iterator();
		Iterator<PEColumn> rightIter = other.getColumns(sc).iterator();
		while(leftIter.hasNext() && rightIter.hasNext()) {
			PEColumn lc = leftIter.next();
			PEColumn rc = rightIter.next();
			if (lc.collectDifferences(sc,messages, rc, first, visited))
				return true;
		}
		return false;
	}

	private boolean compareKeys(SchemaContext sc, List<String> messages, PETable other,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		if (maybeBuildDiffMessage(sc,messages, "number of keys", getKeys(sc).size(), other.getKeys(sc).size(), first, visited))
			return true;
		Iterator<PEKey> leftIter = getKeys(sc).iterator();
		Iterator<PEKey> rightIter = getKeys(sc).iterator();
		while(leftIter.hasNext() && rightIter.hasNext()) {
			PEKey lk = leftIter.next();
			PEKey rk = rightIter.next();
			if (lk.collectDifferences(sc, messages, rk, first, visited))
				return true;
		}
		return false;
	}
	
	@Override
	protected String getDiffTag() {
		return "Table";
	}

	@Override
	public String differs(SchemaContext sc, Persistable<PETable, UserTable> other, boolean first) {
		checkLoaded(sc);
		return super.differs(sc,other, first);
	}

	public String definitionDiffers(PETable other) {
		String mine = getDefinition();
		String yours = other.getDefinition();
		if (mine != null && yours != null) {
			if (!mine.equals(yours)) {
				return "table definitions differ";
			}
			return null;
		} else if (mine == null && yours == null) {
			return null;
		} else {
			return "missing table definition";
		}
	}
	
	@Override
	public String toString() {
		return getName().get();
	}

	@Override
	protected void populateNew(SchemaContext pc,UserTable p) throws PEException {
		super.populateNew(pc,p);
		for(PEKey pek : getKeys(pc)) {
			p.addKey(pek.persistTree(pc));
		}
		setModifiers(pc,p);
	}

	@Override
	protected Persistable<PETable, UserTable> load(SchemaContext pc, UserTable p)
			throws PEException {
		return new PETable(p,pc);
	}
	
	private void setModifiers(SchemaContext pc, UserTable p) throws PEException {
		CollationTableModifier collation = (CollationTableModifier) modifiers.getModifier(TableModifierTag.DEFAULT_COLLATION);
		EngineTableModifier etm = (EngineTableModifier) modifiers.getModifier(TableModifierTag.ENGINE);
		RowFormatTableModifier rtfm = (RowFormatTableModifier) modifiers.getModifier(TableModifierTag.ROW_FORMAT);
		CommentTableModifier ctm = (CommentTableModifier) modifiers.getModifier(TableModifierTag.COMMENT);
		if (collation != null)
			p.setCollation(collation.getCollation().getUnquotedName().get());
		if (rtfm != null)
			p.setRowFormat(rtfm.getRowFormat().getSQL());
		if (etm != null)
			p.setEngine(etm.getPersistent());
		if (ctm != null)
			p.setComment(ctm.getComment().getComment());
		p.setCreateOptions(CreateOptionModifier.build(CreateOptionModifier.combine(CreateOptionModifier.decode(p.getCreateOptions()), modifiers)));		
	}
		
	@Override
	protected void updateExisting(SchemaContext pc, UserTable ut) throws PEException {
		super.updateExisting(pc,ut);
		updateExistingKeys(pc,ut);
		
		setModifiers(pc,ut);
	}

	private static void updateColumnPositions(final SchemaContext sc, final PEKey source, final PEKey in) {
		final List<PEColumn> updated = in.getColumns(sc);
		for (final PEColumn sourceCol : source.getColumns(sc)) {
			final int index = updated.indexOf(sourceCol);
			if (index > -1) {
				final PEColumn updatedCol = updated.get(index);
				updatedCol.setPosition(sourceCol.getPosition());
			}
		}
	}

	protected void updateExistingKeys(SchemaContext pc, UserTable ut) throws PEException {
		HashMap<String, Key> persKeys = new HashMap<String, Key>();
		HashMap<String, Key> persCons = new HashMap<String, Key>();
		HashMap<String, PEKey> transKeys = new HashMap<String, PEKey>();
		HashMap<String, PEForeignKey> transCons = new HashMap<String, PEForeignKey>();
		for(PEKey c : getKeys(pc)) {
			if (c.isForeign())
				transCons.put(c.getName().getCapitalized().get(), (PEForeignKey)c);
			else
				transKeys.put(c.getName().getCapitalized().get(), c);
		}
		for(Key uc : ut.getKeys()) {
			String name = uc.getName().toUpperCase().trim();
			if (uc.isForeignKey()) {
				PEForeignKey was = transCons.remove(name);
				boolean same = (was != null);
				if (same) {
					PEForeignKey apc = PEForeignKey.load(uc, pc, null);
					updateColumnPositions(pc, was, apc);
					String anydiffs = was.differs(pc,apc, true);
					if (anydiffs != null) {
						same = false;
						transCons.put(name, was);
					}
				}
				if (!same)
					persCons.put(name,uc);
			} else {
				PEKey was = transKeys.remove(name);
				boolean same = (was != null);
				if (same) {
					PEKey apc = PEKey.load(uc, pc, null);
					updateColumnPositions(pc, was, apc);
					String anydiffs = was.differs(pc,apc, true);
					if (anydiffs != null) {
						same = false;
						transKeys.put(name, was);
					}
				}
				if (!same)
					persKeys.put(name,uc);
			}
		}
		// now transCols has columns not in persCols, and persCols has columns not in transCols
		// the former are additions, the latter are removals
		for(Key uc : persCons.values()) {
			ut.removeKey(uc);
		}
		for(Key uc : persKeys.values()) {
			ut.removeKey(uc);
		}
		pc.beginSaveContext();
		try {
			for(PEKey c : transKeys.values()) {
				ut.addKey(c.persistTree(pc));
			}
			for(PEForeignKey c : transCons.values()) {
				ut.addKey(c.persistTree(pc));
			}
		} finally {
			pc.endSaveContext();
		}
	}

	
	public boolean hasAutoInc() {
		return (Boolean.TRUE.equals(hasAutoInc));
	}

	private void forceStorage(SchemaContext pc) {
		EngineTableModifier etm = (EngineTableModifier) modifiers.getModifier(TableModifierTag.ENGINE);
		if (etm == null) {
			String engine = SchemaVariables.getStorageEngine(pc); 
			if ( engine != null)
				etm = new EngineTableModifier(EngineTag.findEngine(engine));
			else
				etm = new EngineTableModifier(EngineTableModifier.EngineTag.INNODB);
			modifiers.setModifier(etm);
		}
	}

	public void alterModifier(TableModifier tm) {
		if (tm.getKind() == null)
			throw new SchemaException(Pass.PLANNER, "Unknown table modifier kind, unable to alter");
		modifiers.setModifier(tm);
	}
	
	// we don't store a complete orig decl in any one place, but instead recreate it
	// this function does that - take the decl and apply any changes (autoincs, nonpersisted fks, synthetic keys)
	// to the decl then return a new tschema copy
	@Override
	public PETable recreate(SchemaContext sc, String decl, LockInfo li) {
		PETable tschemaVersion = super.recreate(sc,decl, li).asTable();
		boolean mtmode = tschemaVersion.getPEDatabase(sc).getMTMode().isMT();
		
		// correctly
		boolean mod = false;
		for(PEColumn c : getColumns(sc)) {
			if (c.isAutoIncrement()) {
				PEColumn ntc = (PEColumn) c.getIn(sc,tschemaVersion);
				ntc.makeAutoincrement();
				mod = true;
			} else if (c.getType().isBinaryText()) {
				// we don't store this in the declaration, but it is in the catalog
				PEColumn ntc = c.getIn(sc, tschemaVersion);
				ntc.makeBinaryText();
			}
		}
		if (mtmode) {
			HashMap<UnqualifiedName,PEForeignKey> tfks = new HashMap<UnqualifiedName,PEForeignKey>();
			for(PEKey pek : tschemaVersion.getKeys(sc)) {
				if (!pek.isForeign()) continue;
				PEForeignKey pefk = (PEForeignKey) pek;
				tfks.put(pefk.getPhysicalSymbol(), pefk);
			}
			for(PEKey pek : getKeys(sc)) {
				if (!pek.isForeign()) continue;
				PEForeignKey pefk = (PEForeignKey) pek;
				PEForeignKey tfk = tfks.get(pefk.getPhysicalSymbol());
				if (tfk != null) {
					tfk.setSymbol(pefk.getSymbol());
					tfk.setPhysicalSymbol(pefk.getPhysicalSymbol());
					mod = true;
				}
			}
		}
		if (mod) 
			tschemaVersion.setDeclaration(sc,tschemaVersion);
		// there is nothing in the create tbl stmt that can indicate that a key is synthetic, so if
		// we have any synthetic keys in this table, so mark them in the recreated table
		// also add in any fks that we don't actually persist - note that we have to make the order match, so we will
		// mess around with the list of keys directly in the tschema version
		List<PEKey> myKeys = getKeys(sc);
		for(int i = 0; i < myKeys.size(); i++) {
			PEKey pek = myKeys.get(i);
			if (pek.isSynthetic()) {
				PEKey npek = pek.getIn(sc, tschemaVersion);
				npek.setSynthetic();
			} else if (pek.isForeign()) {
				PEForeignKey pefk = (PEForeignKey) pek;
				if (!pefk.isPersisted()) {
					// need to copy it in
					PEForeignKey npefk = (PEForeignKey) pefk.copy(sc, tschemaVersion);
					npefk.setPersisted(false);
					tschemaVersion.keys.add(i, npefk);
				} 
			}
		}
		
		return tschemaVersion;
	}
	
	public Integer getAutoIncrTrackerID() {
		return autoIncTrackerID;
	}
	
	@Override
	public void checkValid(SchemaContext sc, List<ValidateResult> acc) {
		for(PEKey pek : getKeys(sc)) {
			if (pek.isForeign()) {
				PEForeignKey pefk = (PEForeignKey) pek;
				pefk.checkValid(sc, acc);
			}
		}
	}
	
	public static boolean valid(UserTable ut) {
		if (ut.getReferringKeys() == null)
			return false;
		if (ut.getKeys() == null)
			return false;
		if (!ut.getReferringKeys().isEmpty())
			ut.getReferringKeys().iterator().next();
		if (!ut.getKeys().isEmpty())
			ut.getKeys().iterator().next();
		return true;
	}

	@Override
	public String getTableType() {
		return PEConstants.DEFAULT_TABLE_TYPE;
	}

	public PersistentTable getPersistentTable(SchemaContext sc) {
		return cached;
	}

	protected static class CachedPETable implements PersistentTable {

		private final PETable table;
		private final PersistentDatabase db;
		private final StorageGroup pg;
		private final PersistentContainer container;
		private final Integer rangeID;

		public CachedPETable(SchemaContext sc, PETable tab) {
			this.table = tab;
			this.db = tab.getPEDatabase(sc);
			this.container = tab.getDistributionVector(sc).getContainer(sc);
			if (tab.getPersistentStorage(sc) == null)
				this.pg = null;
			else
				this.pg = tab.getPersistentStorage(sc).getScheduledGroup(sc);
			rangeID = tab.getDistributionVector(sc).getRangeID(sc);
		}
		
		@Override
		public String displayName() {
			return getPersistentName();
		}

		@Override
		public String getNameAsIdentifier() {
            return Singletons.require(HostService.class).getDBNative().getNameForQuery(this);
		}

		@Override
		public String getPersistentName() {
			return table.getName().getUnqualified().getUnquotedName().get();
		}

		@Override
		public String getQualifiedName() {
			if (db == null)
				return getPersistentName();
			return db.getUserVisibleName() + "." + getPersistentName();
		}

		@Override
		public int getNumberOfColumns() {
			return table.getColumns(null).size();
		}

		@Override
		public KeyValue getDistValue(CatalogDAO c) {
			KeyValue dv = new KeyValue(this,rangeID);
			TreeMap<Integer,PersistentColumn> sorted =new TreeMap<Integer,PersistentColumn>();
			for(PEColumn pec : table.getColumns(null))
				if (pec.getHashPosition() > 0)
					sorted.put(pec.getHashPosition(),pec);
			for(PersistentColumn pc : sorted.values())
				dv.addColumnTemplate(pc);
			return dv;
		}

		@Override
		public StorageGroup getPersistentGroup() {
			return pg;
		}

		@Override
		public DistributionModel getDistributionModel() {
			return table.dv.getModel().getSingleton();
		}

		@Override
		public int getId() {
			if (table.getPersistentID() == null)
				return 0;
			return table.getPersistentID();
		}

		@Override
		public PersistentContainer getContainer() {
			return container;
		}

		@Override
		public PersistentDatabase getDatabase() {
			return db;
		}

		@Override
		public PersistentColumn getUserColumn(String name) {
			return table.lookup(null, name);
		}

		@Override
		public Integer getRangeID(CatalogDAO c) throws PEException {
			return rangeID;
		}
		
	}
	
	public long getTableSizeEstimate(SchemaContext sc) {
		for(PEKey pek : getUniqueKeys(sc)) {
			long card = pek.getCardinality();
			if (card > -1) return card;
		}
		return -1;
	}
	
	@Override
	public boolean hasCardinalityInfo(SchemaContext sc) {
		if (hasCardInfo == null) 
			hasCardInfo = computeCardInfo(sc);
		return hasCardInfo.booleanValue();		
	}
	
	private boolean computeCardInfo(SchemaContext sc) {
		for(PEKey pek : getKeys(sc)) {
			if (pek.isForeign()) continue;
			if (pek.getCardinality() == -1) return false;
		}
		return true;
	}
	
	public boolean isExplicitlyDeclared() {
		return false;
	}
	
	public boolean mustBeCreated() {
		return false;
	}
}
