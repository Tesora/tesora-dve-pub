package com.tesora.dve.sql.statement.ddl;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.TemporaryTable;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerCreateDatabaseRequest;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEForeignKeyColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEKeyColumnBase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.statement.ddl.alter.DropIndexAction;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.SessionExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;
import com.tesora.dve.worker.WorkerGroup;

public class PECreateTableStatement extends
		PECreateStatement<PETable, UserTable> {

	List<DelayedFKDrop> related = null;
	List<TableCacheKey> alsoClear = null;
	Set<PEForeignKey> transModified = null;
	
	public PECreateTableStatement(Persistable<PETable, UserTable> targ, boolean exists) {
		super(targ, false,"TABLE",exists);
	}
	
	public PECreateTableStatement(Persistable<PETable, UserTable> targ, Boolean ine, boolean exists) {
		super(targ, false, ine, "TABLE", exists);
	}	
	
	@SuppressWarnings("unchecked")
	public PECreateTableStatement(PECreateTableStatement base) {
		super((Persistable<PETable, UserTable>) base.getRoot(), false, base.isIfNotExists(), "TABLE", !base.isNew());
	}
	
	@Override
	public PEDatabase getDatabase(SchemaContext pc) {
		return ((PETable)getRoot()).getPEDatabase(pc);
	}
	
	public PETable getTable() {
		return ((PETable)getRoot());
	}

	@Override
	protected void preplan(SchemaContext pc, ExecutionSequence es,boolean explain) throws PEException {
		if (getTable().isUserlandTemporaryTable())
			return;
		super.preplan(pc, es, explain);
	}
	
	protected static void addIgnoredFKMessage(SchemaContext sc, ValidateResult vr) {
		sc.getConnection().getMessageManager().addWarning(vr.getMessage(sc) + " - not persisted");
	}
	
	// for trans exec engine only - what keys did we modify?
	@SuppressWarnings("unchecked")
	public Set<PEForeignKey> getModifiedKeys() {
		return (transModified == null ? Collections.EMPTY_SET : transModified);
	}
	
	// so, this normalize basically just does checking for validity - all the actual changes happen in the callbacks
	@Override
	public void normalize(SchemaContext pc) {
		if (related == null) {
			if (pc.isPersistent()) {
				List<ValidateResult> results = getTable().validate(pc,false);
				// make sure we fail on errors
				for(ValidateResult vr : results) {
					if (vr.isError())
						throw new SchemaException(Pass.NORMALIZE,vr.getMessage(pc));
				}
			}
			// also look for dangling fks, but not for temp tables
			if (!getTable().isUserlandTemporaryTable()) {
				DanglingFKComputeBefore computer = new DanglingFKComputeBefore(pc.isPersistent());
				computer.computeDanglingFKs(pc, getTable());
				related = computer.getDrops();
				// need to add the ref'd tables to the clear list in order to force
				// reload for fks
				alsoClear = computeRefdTables(pc);
				transModified = computer.getModded();
			} else {
				related = Collections.emptyList();
				alsoClear = Collections.emptyList();
			}
		}
	}
					
	private abstract static class DanglingFKCompute {
		
		protected abstract void begin(PETable pet);
		protected abstract void onMatch(PETable pet, PEForeignKey pefk);
		protected abstract void end(SchemaContext sc, PETable pet);
		
		protected void computeDanglingFKs(SchemaContext sc, PETable newTab) {
			boolean required = SchemaVariables.hasForeignKeyChecks(sc);
			UnqualifiedName dbName = newTab.getDatabase(sc).getName().getUnqualified();
			UnqualifiedName tabName = newTab.getName().getUnqualified();
			List<PETable> matching = sc.findTablesWithUnresolvedFKSTargeting(dbName, tabName);
			for(PETable pet : matching) {
				begin(pet);
				for(PEKey pek : pet.getKeys(sc)) {
					if (pek.isForeign()) {
						PEForeignKey pefk = (PEForeignKey) pek;
						if (pefk.isForward() && pefk.getTargetTableName(sc).equals(tabName)) {
							for(PEKeyColumnBase pekc : pefk.getKeyColumns()) {
								PEForeignKeyColumn pefkc = (PEForeignKeyColumn)pekc;
								PEColumn tc = newTab.lookup(sc, pefkc.getTargetColumnName());
								if (tc != null) {
									pefkc.setTargetColumn(tc);
									pefk.setTargetTable(sc, newTab);
									onMatch(pet,pefk);
								}
								else if (required) {
									// TODO: should we make this conditional on the var
									throw new SchemaException(Pass.NORMALIZE, 
											"No such column: " + pefkc.getTargetColumnName() 
											+ " in table " + tabName 
											+ " required for foreign key in table " + pefk.getTable(sc).getName().getUnquotedName());
								}
							}
						}
					}
				}
				end(sc,pet);
			}
		}
		
	}
	
	private static class DanglingFKComputeBefore extends DanglingFKCompute {

		private List<DelayedFKDrop> drops = new ArrayList<DelayedFKDrop>();
		private Set<PEForeignKey> allModdedKeys = new LinkedHashSet<PEForeignKey>();
		private Set<PEForeignKey> moddedKeys = null;
		private boolean complain;
		
		public DanglingFKComputeBefore(boolean complain) {
			this.complain = complain;
		}
		
		public List<DelayedFKDrop> getDrops() { return drops; }
		
		public Set<PEForeignKey> getModded() { return allModdedKeys; }
		
		@Override
		protected void begin(PETable pet) {
			moddedKeys = new LinkedHashSet<PEForeignKey>();
		}

		@Override
		protected void onMatch(PETable pet, PEForeignKey pefk) {
			moddedKeys.add(pefk);
		}

		@Override
		protected void end(SchemaContext sc, PETable pet) {
			if (!moddedKeys.isEmpty()) {
				allModdedKeys.addAll(moddedKeys);
				List<ValidateResult> results = pet.validate(sc, false);
				LinkedHashSet<PEForeignKey> toDrop = new LinkedHashSet<PEForeignKey>();
				for(ValidateResult vr : results) {
					if (vr.isError() && complain)
						throw new SchemaException(Pass.NORMALIZE,vr.getMessage(sc));
					else if (vr.getSubject() instanceof PEForeignKey) {
						PEForeignKey pefk = (PEForeignKey) vr.getSubject();
						toDrop.add(pefk);
						addIgnoredFKMessage(sc,vr);
					}
				}
				List<KeyID> ids = Functional.apply(toDrop, new UnaryFunction<KeyID, PEForeignKey>() {

					@Override
					public KeyID evaluate(PEForeignKey object) {
						return new KeyID(object);
					}
					
				});
				if (!ids.isEmpty()) {
					drops.add(new DelayedFKDrop(pet, ids));
				}
			}
			moddedKeys = null;
		}
		
	}
	
	private static class DanglingFKComputeAfter extends DanglingFKCompute {

		private List<PETable> tables = new ArrayList<PETable>();
		private boolean any;
		
		public DanglingFKComputeAfter() {
			
		}
		
		public List<PETable> getTables() {
			return tables;
		}
		
		@Override
		protected void begin(PETable pet) {
			any = false;
		}

		@Override
		protected void onMatch(PETable pet, PEForeignKey pefk) {
			any = true;
		}

		@Override
		protected void end(SchemaContext sc, PETable pet) {
			if (any)
				tables.add(pet);
		}
		
	}
	
	
	protected List<TableCacheKey> computeRefdTables(SchemaContext pc) {
		List<TableCacheKey> out = new ArrayList<TableCacheKey>();
		for(PEKey pek : getTable().getKeys(pc)) {
			if (!pek.isForeign()) continue;
			PEForeignKey pefk = (PEForeignKey) pek;
			if (pefk.isForward()) continue;
			out.add((TableCacheKey) pefk.getTargetTable(pc).getCacheKey());
		}
		return out;
	}
	
	// if none of the dangling fks need further modification, execute a single step whose ddl
	// is the create and whose ents are all the modded tables.
	
	// if any of the dangling fks need further modification, execute drops as needed in a ddl callback
	// then execute the original create and regenerate the ents in a ddl callback
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		normalize(pc);
		// we may just need a single step, but we may not - figure that out
		if (alreadyExists) {
			es.append(new EmptyExecutionStep(0,"already exists - " + getSQL(pc)));
			return;
		}
		boolean immediate = !pc.isPersistent() ||
				getTable().isUserlandTemporaryTable() || 
				Functional.all(related, new UnaryPredicate<DelayedFKDrop>() {

					@Override
					public boolean test(DelayedFKDrop object) {
						return object.getKeyIDs().isEmpty();
					}
					
				});
		
		maybeDeclareDatabase(pc,es);

		if (immediate) {
			oneStepPlan(pc,es);
		} else {
			manyStepPlan(pc,es);
		}		
	}
	
	protected void manyStepPlan(SchemaContext pc, ExecutionSequence es) throws PEException {
		List<TableCacheKey> modded = new ArrayList<TableCacheKey>();
		PETable tab = getTable();
		// we always do the drops beforehand
		for(DelayedFKDrop dfd : related) {
			if (!dfd.getKeyIDs().isEmpty()) {
				es.append(new ComplexDDLExecutionStep(tab.getPEDatabase(pc),
						getTable().getStorageGroup(pc),null,Action.ALTER,dfd));
			}
			modded.add(dfd.getEnclosingTableKey());
		}
		// and then we do the create
		es.append(new ComplexDDLExecutionStep(tab.getPEDatabase(pc),
				tab.getStorageGroup(pc),null,Action.CREATE,
				new CreateTableCallback(tab,alsoClear,modded)));		
	}
	
	protected void oneStepPlan(SchemaContext pc, ExecutionSequence es) throws PEException {		
		// so - we need to go back and finalize the normalization
		PETable tab = getTable();
		// if it's a userland temporary table, no need to muck around with fks
		if (tab.isUserlandTemporaryTable()) {
			if (es != null) {
				es.append(new ComplexDDLExecutionStep(getDatabase(pc),getStorageGroup(pc),tab,Action.CREATE,
						new CreateTemporaryTableCallback(pc,(ComplexPETable)tab,getSQLCommand(pc))));				
			}
			return;
		}
		
		
		boolean mustRebuildCTS = false;
		List<ValidateResult> results = tab.validate(pc,false);
		for(ValidateResult vr : results) {
			if (vr.isError()) continue;
			if (vr.getSubject() instanceof PEForeignKey) {
				PEForeignKey pefk = (PEForeignKey) vr.getSubject();
				pefk.setPersisted(false);
				addIgnoredFKMessage(pc,vr);
				mustRebuildCTS = true;
			}
		}
		if (mustRebuildCTS) 
			// we need to rebuild the create table stmt
			tab.setDeclaration(pc, tab);
		ListOfPairs<SchemaCacheKey<?>,InvalidationScope> clears = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
		if (!tab.isUserlandTemporaryTable())
			clears.add(tab.getCacheKey(),InvalidationScope.LOCAL);
		List<CatalogEntity> updates = null;
		pc.beginSaveContext();
		try {
			tab.persistTree(pc);
			pc.getSource().setLoaded(tab, tab.getCacheKey());
			for(DelayedFKDrop dfd : related) {
				dfd.getTable().persistTree(pc);
				clears.add(dfd.getTable().getCacheKey(),InvalidationScope.LOCAL);
			}
			updates = Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
		for(TableCacheKey tck : alsoClear)
			clears.add(tck, InvalidationScope.LOCAL);
		if (es != null)
			es.append(new SimpleDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), tab, getAction(),
				getSQLCommand(pc), getDeleteObjects(pc), updates,
				new CacheInvalidationRecord(clears)));
	}
	
	protected void maybeDeclareDatabase(SchemaContext sc, ExecutionSequence es) throws PEException {
		// when the storage group specified on the table is not the database default, the database might not actually
		// have been declared on the storage group.  in that case, we need to send along a create database (without the 
		// ddl callback) so that the create table will work.  this used to be transparent because we always were doing a
		// create database
		if (getTable().getPersistentStorage(sc).getCacheKey().equals(getTable().getDatabase(sc).getDefaultStorage(sc).getCacheKey()))
			return;
		final PEDatabase pedb = getTable().getPEDatabase(sc);
		es.append(new TransientSessionExecutionStep(null, getTable().getPersistentStorage(sc), "create database on non default group",false,true,
				
					new AdhocOperation() {

						@Override
						public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
								throws Throwable {

							WorkerCreateDatabaseRequest req = new WorkerCreateDatabaseRequest(ssCon.getTransactionalContext(),pedb,true);
							wg.execute(WorkerGroup.MappingSolution.AllWorkers, req, resultConsumer);

						}
					}));
	}
	
	@Override
	public StatementType getStatementType() {
		return StatementType.CREATE_TABLE;
	}
		
	@Override
	public boolean filterStatement(SchemaContext pc) {
		PETable tbl = (PETable)getCreated();
		if (tbl != null) {
			// temporary table
			if (tbl.getDatabase(pc) == null) return false;
			if (pc.getConnection().isFilteredTable(
					new QualifiedName(
							tbl.getDatabase(pc).getName().getUnquotedName().getUnqualified(),
							tbl.getName().getUnquotedName().getUnqualified()))) {
				return true;
			}
		}
		return false;
	}

	static class KeyID {
		
		private ConstraintType constraintType;
		private UnqualifiedName symbol;
		private UnqualifiedName name;
		
		public KeyID(PEKey pek) {
			constraintType = pek.getConstraint();
			symbol = pek.getSymbol();
			name = pek.getName().getUnqualified();
		}

		public boolean matches(PEKey pek) {
			if (constraintType != pek.getConstraint()) return false;
			if (symbol != null && pek.getSymbol() != null && !symbol.equals(pek.getSymbol())) return false;
			return name.equals(pek.getName().getUnqualified());
		}
		
		public PEKey findIn(SchemaContext sc, PETable pet) {
			for(PEKey pek : pet.getKeys(sc)) {
				if (matches(pek))
					return pek;
			}
			return null;
		}
		
	}
	
	static class DelayedFKDrop extends NestedOperationDDLCallback {
		
		private PETable enclosingTable;
		private List<KeyID> keysToDrop;
		private List<CatalogEntity> updates;
		private List<QueryStep> ddl;
		private CacheInvalidationRecord record;
		
		public DelayedFKDrop(PETable tab, List<KeyID> keys) {
			enclosingTable = tab;
			keysToDrop = keys;
			updates = null;
			ddl = null;
			record = new CacheInvalidationRecord(enclosingTable.getCacheKey(),InvalidationScope.LOCAL);
		}

		public TableCacheKey getEnclosingTableKey() {
			return (TableCacheKey) enclosingTable.getCacheKey();
		}
		
		public PETable getTable() {
			return enclosingTable;
		}
		
		public List<KeyID> getKeyIDs() {
			return keysToDrop;
		}
		
		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			conn.getCatalogDAO();
			// just create a new schema context
			SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();
			updates = null;
			ddl = null;
			
			// load my table
			PETable tab = (PETable) enclosingTable.getCacheKey().load(sc);
			// load the keys I'm going to toss
			List<PEForeignKey> toDrop = new ArrayList<PEForeignKey>();
			for(KeyID ki : keysToDrop) {
				PEKey pek = ki.findIn(sc, tab);
				if (pek == null) continue;
				if (!pek.isForeign()) continue;
				toDrop.add((PEForeignKey) pek);
			}
			// now figure out the drop order
			ListSet<PEKey> dropped = new ListSet<PEKey>();
			MultiMap<PEKey,PEForeignKey> dependents = new MultiMap<PEKey,PEForeignKey>();
			HashMap<PEForeignKey,PEKey> reverse = new HashMap<PEForeignKey,PEKey>();
			for(PEForeignKey pefk : tab.getForeignKeys(sc)) {
				PEKey prefix = pefk.findPrefixKey(sc, tab);
				dependents.put(prefix, pefk);
				reverse.put(pefk,prefix);
			}
			for(PEForeignKey pefk : toDrop) {
				PEKey dependsOn = reverse.get(pefk);
				dependents.remove(dependsOn,pefk);
				Collection<PEForeignKey> sub = dependents.get(dependsOn);
				if (sub == null || sub.isEmpty())
					dropped.add(dependsOn);
			}
			dropped.addAll(toDrop);
			// don't drop stuff that is already not persisted - the ddl will fail
			for(Iterator<PEKey> iter = dropped.iterator(); iter.hasNext();) {
				PEKey pek = iter.next();
				if (!pek.isPersisted())
					iter.remove();
			}
			
			// the dml is going to be one alter/drop for each key in the record
			// there are no deletes, I don't think
			ExecutionSequence es = new ExecutionSequence(null);
			for(PEKey pek : dropped) {
				pek.setPersisted(false);
				PEAlterTableStatement peat = 
						new PEAlterTableStatement(sc, TableKey.make(sc,tab, sc.getNextTable()), Collections.singletonList(new DropIndexAction(pek)
								.markTransientOnly()));
				String sql = peat.getSQL(sc);
				SessionExecutionStep ses = new SessionExecutionStep(tab.getDatabase(sc),tab.getStorageGroup(sc), sql);
				es.append(ses);
			}
			tab.setDeclaration(sc,tab);
			ddl = new ArrayList<QueryStep>();
			es.schedule(null, ddl, null, sc);
			sc.beginSaveContext();
			try {
				// the 'right' way to do this would be to put the persisted flag into the differs map
				// but I'm not entirely sure that will work correctly right now
				for(PEKey pek : dropped)
					pek.persistTree(sc);
				tab.persistTree(sc);
				sc.getSource().setLoaded(tab, tab.getCacheKey());
				updates = Functional.toList(sc.getSaveContext().getObjects());
			} finally {
				sc.endSaveContext();
			}
		}

		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updates;
		}

		@Override
		public boolean canRetry(Throwable t) {
			return false;
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		@Override
		public String description() {
			return "ForwardDecl FK drops for ignore mode on " + enclosingTable.getCacheKey();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
			if (ddl != null) {
				for(QueryStep qs : ddl) {
					QueryStepOperation qso = qs.getOperation();
					qso.execute(conn, wg, DBEmptyTextResultConsumer.INSTANCE);
				}			
			}
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

	}
		
	static class CreateTableCallback extends DDLCallback {

		private PETable builtVersion;
		private CacheInvalidationRecord record;
		private List<CatalogEntity> updates;
		private SQLCommand sql;
		
		public CreateTableCallback(PETable translated, List<TableCacheKey> referring, List<TableCacheKey> modified) {
			builtVersion = translated;
			ListOfPairs<SchemaCacheKey<?>, InvalidationScope> invalidate = 
					new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
			invalidate.add(builtVersion.getCacheKey(), InvalidationScope.CASCADE);
			for(TableCacheKey tck : modified)
				invalidate.add(tck, InvalidationScope.CASCADE);
			for(TableCacheKey tck : referring)
				invalidate.add(tck, InvalidationScope.LOCAL);
			record = new CacheInvalidationRecord(invalidate);
			sql = new SQLCommand(builtVersion.getDeclaration());
		}
		
		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			conn.getCatalogDAO();
			// just create a new schema context
			SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();
			sc.setOptions(ParserOptions.NONE.setResolve());
			updates = null;
			sql = null;

			PETable fresh = builtVersion.recreate(sc, builtVersion.getDeclaration(), 
					new LockInfo(LockType.EXCLUSIVE, "create table"));
			// do the stuff we used do during normalization
			boolean mustRebuildCTS = false;
			List<ValidateResult> results = fresh.validate(sc,false);
			for(ValidateResult vr : results) {
				if (vr.isError()) continue;
				if (vr.getSubject() instanceof PEForeignKey) {
					PEForeignKey pefk = (PEForeignKey) vr.getSubject();
					pefk.setPersisted(false);
					addIgnoredFKMessage(sc,vr);
					mustRebuildCTS = true;
				}
			}
			if (mustRebuildCTS) 
				// we need to rebuild the create table stmt
				fresh.setDeclaration(sc, fresh);
			DanglingFKComputeAfter afterComp = new DanglingFKComputeAfter();
			afterComp.computeDanglingFKs(sc, fresh);
			List<PETable> modded = afterComp.getTables();
			sc.beginSaveContext();
			try {
				fresh.persistTree(sc);
				sc.getSource().setLoaded(fresh, fresh.getCacheKey());
				for(PETable pet : modded)
					pet.persistTree(sc);
				updates = Functional.toList(sc.getSaveContext().getObjects());
			} finally {
				sc.endSaveContext();
			}
			sql = new SQLCommand(fresh.getDeclaration());
		}

		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updates;
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return sql;
		}

		@Override
		public boolean canRetry(Throwable t) {
			return false;
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		@Override
		public String description() {
			return sql.toString();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

	}

	private static class CreateTemporaryTableCallback extends DDLCallback {

		SQLCommand stmt;
		// we need to use this specific context to populate the temporary table schema
		SchemaContext context;
		// and the table
		ComplexPETable table;

		int pincount = 0;
		
		List<CatalogEntity> updates;
		
		public CreateTemporaryTableCallback(SchemaContext sc, ComplexPETable table, SQLCommand stmt) {
			this.context = sc;
			this.table = table;
			this.stmt = stmt;
			this.updates = null;
		}
		
		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			this.updates = new ArrayList<CatalogEntity>();
			updates.add(new TemporaryTable(table.getName().getUnqualified().getUnquotedName().get(),
					table.getPEDatabase(context).getName().getUnqualified().getUnquotedName().get(),
					table.getEngine().getPersistent(),
					conn.getConnectionId()));
		}
		
		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException
		{
			return updates;
		}
		
		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return stmt;
		}

		@Override
		public String description() {
			return stmt.getRawSQL();
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		
		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return null;
		}
		
		public void onCommit(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			pincount++;
			if (pincount == 1) {
				wg.markPinned();
				context.getTemporaryTableSchema().addTable(context, table);
				// if we already have it we won't do it again
				conn.acquireInflightTemporaryTablesLock();
			}
		}
		
		public void onRollback(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			if (pincount == 1) {
				wg.clearPinned();
				pincount--;
				context.getTemporaryTableSchema().removeTable(context, table);
				if (context.getTemporaryTableSchema().isEmpty())
					conn.releaseInflightTemporaryTablesLock();
			}
		}

	}
	
}
