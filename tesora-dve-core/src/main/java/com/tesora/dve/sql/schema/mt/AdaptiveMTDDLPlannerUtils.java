package com.tesora.dve.sql.schema.mt;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.common.catalog.Shape;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEForeignKeyColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEKeyColumn;
import com.tesora.dve.sql.schema.PEKeyColumnBase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.mt.PETenant.TenantCacheKey;
import com.tesora.dve.sql.schema.mt.TableScope.ScopeCacheKey;
import com.tesora.dve.sql.statement.ddl.PEAlterTableStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTenantTableStatement;
import com.tesora.dve.sql.statement.ddl.ShapeLock;
import com.tesora.dve.sql.statement.ddl.alter.AddIndexAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.DropIndexAction;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.SessionExecutionStep;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.worker.WorkerGroup;

public final class AdaptiveMTDDLPlannerUtils {

	public static void addDDLCallback(SchemaContext sc, PEDatabase pdb, PEStorageGroup psg,
			Persistable<?,?> theRoot,
			Action theAction,
			ExecutionSequence es, QueryStepDDLGeneralOperation.DDLCallback cb,
			Long updateCountOverride) {
		ComplexDDLExecutionStep step = new ComplexDDLExecutionStep(pdb, psg, theRoot, theAction, cb);
		if (updateCountOverride != null)
			step.setUpdateCountOverride(updateCountOverride);
		es.append(step);
	}
	
	public static CreateTableOperation 
		addCreateTable(SchemaContext sc, ExecutionSequence es,
				PETenant theTenant,
				PETable theTable, UnqualifiedName visibleName, 
				TableState requestedState,
				Map<UnqualifiedName,LateFKFixup> lateFixups) {
		CreateTableOperation csto = new CreateTableOperation(sc, theTable,visibleName, theTenant, requestedState, lateFixups);
		addDDLCallback(sc,theTable.getPEDatabase(sc), theTable.getStorageGroup(sc),
				theTable, Action.CREATE, es,csto, null);
		return csto;
	}
	
	// when a table is referenced and the referencing fk has a set null action, we need to ensure that the
	// target table has a naked index
	public static void handleSetNullActions(final SchemaContext sc, ExecutionSequence es, 
			PETable referring, TableScope referringScope, PETenant onTenant, 
			Map<UnqualifiedName,LateFKFixup> forwarding) throws PEException {
		ListOfPairs<PETable,PEForeignKey> referringKeys = new ListOfPairs<PETable,PEForeignKey>();
		ListOfPairs<PETable,PEKey> added = new ListOfPairs<PETable,PEKey>();
		for(PEKey pek : referring.getKeys(sc)) {
			if (!pek.isForeign()) continue;
			PEForeignKey pefk = (PEForeignKey) pek;
			if (pefk.isForward()) continue;
			Pair<PEForeignKey,PEKey> nk = maybeRequiresNewKey(sc,pefk);
			if (nk != null) {
				PETable target = nk.getFirst().getTargetTable(sc); 
				added.add(target,nk.getSecond());
				referringKeys.add(target,nk.getFirst());
			}
		}
		if (added.isEmpty()) return;
		for(int i = 0; i < added.size(); i++) {
			Pair<PETable,PEKey> nkp = added.get(i);
			Pair<PETable,PEForeignKey> nsp = referringKeys.get(i);
			AlterTableAction ata = new AddIndexAction(nkp.getSecond());
			TableKey ntk = new TableKey(nkp.getFirst(),sc.getNextTable());
			PEAlterTableStatement peats = new PEAlterTableStatement(sc,ntk,ata);
			PEAlterTenantTableStatement peatts = new PEAlterTenantTableStatement(sc, peats,
					sc.getPolicyContext().getOfTenant(nkp.getFirst().getPersistent(sc)),onTenant);
			peatts.plan(sc, es, sc.getBehaviorConfiguration());
			forwarding.put(nsp.getSecond().getSymbol(),new LateFKFixup(nsp.getSecond().getSymbol(),peatts.getFinalTableName(),"add nontenant key for set null fk"));
		}
	}
	
	public static Pair<PEForeignKey,PEKey> maybeRequiresNewKey(final SchemaContext sc, PEForeignKey pefk) {
		PETable target = pefk.getTargetTable(sc);
		List<PEKeyColumnBase> keyCols = pefk.getKeyColumns();
		List<PEForeignKeyColumn> pefkcs = Functional.apply(keyCols, new UnaryFunction<PEForeignKeyColumn,PEKeyColumnBase>() {

			@Override
			public PEForeignKeyColumn evaluate(PEKeyColumnBase object) {
				return (PEForeignKeyColumn)object;
			}
			
		});
		PEKey matching = null;
		for(PEKey ik : target.getKeys(sc)) {
			if (ik.isForeign()) continue;
			List<PEColumn> cols = ik.getColumns(sc);
			if (cols.size() != pefkcs.size())
				continue;
			boolean all = true;
			for(int i = 0; i < pefkcs.size(); i++) {
				if (!cols.get(i).getName().equals(pefkcs.get(i).getTargetColumnName())) {
					all = false;
					break;
				}
			}
			if (!all) continue;
			matching = ik;
			break;
		}
		if (matching == null) {
			// have to alter the target table to add an index
			List<PEKeyColumnBase> nkc = Functional.apply(pefkcs, new UnaryFunction<PEKeyColumnBase,PEForeignKeyColumn>() {

				@Override
				public PEKeyColumn evaluate(PEForeignKeyColumn object) {
					return new PEKeyColumn(object.getTargetColumn(sc),null,-1L);
				}
				
			});
			PEKey npek = new PEKey(null,IndexType.BTREE,nkc,null);
			// this key is always hidden
			npek.setHidden(true);
			return new Pair<PEForeignKey, PEKey>(pefk,npek);
		}
		return null;
	}

	public static Pair<PEForeignKey,PEKey> maybeRequiresNewKey(final SchemaContext sc, PEForeignKey pefk, PETable target) {
		List<PEKeyColumnBase> keyCols = pefk.getKeyColumns();
		List<PEForeignKeyColumn> pefkcs = Functional.apply(keyCols, new UnaryFunction<PEForeignKeyColumn,PEKeyColumnBase>() {

			@Override
			public PEForeignKeyColumn evaluate(PEKeyColumnBase object) {
				return (PEForeignKeyColumn)object;
			}
			
		});
		PEKey matching = null;
		for(PEKey ik : target.getKeys(sc)) {
			if (ik.isForeign()) continue;
			List<PEColumn> cols = ik.getColumns(sc);
			if (cols.size() != pefkcs.size())
				continue;
			boolean all = true;
			for(int i = 0; i < pefkcs.size(); i++) {
				if (!cols.get(i).getName().equals(pefkcs.get(i).getTargetColumnName())) {
					all = false;
					break;
				}
			}
			if (!all) continue;
			matching = ik;
			break;
		}
		if (matching == null) {
			// have to alter the target table to add an index
			List<PEKeyColumnBase> nkc = new ArrayList<PEKeyColumnBase>();
			for(PEForeignKeyColumn pefkc : pefkcs) {
				UnqualifiedName unq = pefkc.getTargetColumnName();
				PEColumn pec = target.lookup(sc, unq);
				nkc.add(new PEKeyColumn(pec,null,-1L));
			}			
			PEKey npek = new PEKey(null,IndexType.BTREE,nkc,null);
			// this key is always hidden
			npek.setHidden(true);
			return new Pair<PEForeignKey, PEKey>(pefk,npek);
		}
		return null;
	}

	
	public interface LazyAllocatedTable {

		// this will have the right name
		PETable getTable(SchemaContext sc);
		
		String describe();
		
		boolean isWellFormed();
	}
	
	public static class DirectAllocatedTable implements LazyAllocatedTable {

		private final TableCacheKey theKey;
		private final boolean wellFormed;
		
		public DirectAllocatedTable(TableCacheKey tck, boolean wf) {
			theKey = tck;
			wellFormed = wf;
		}
		
		@Override
		public PETable getTable(SchemaContext sc) {
			return theKey.load(sc).asTable();
		}

		@Override
		public String describe() {
			return "DirectAllocatedTable for " + theKey;
		}

		@Override
		public boolean isWellFormed() {
			return wellFormed;
		}

		
	}
	
	// create table operation is always a separate operation because we need the persistent table id
	// so we can reference it elsewhere
	public static class CreateTableOperation extends QueryStepDDLGeneralOperation.DDLCallback implements LazyAllocatedTable {

		private final PETable definition;
		private final PETenant originator;
		private final TableState requestedState;
		private TableCacheKey targetTable;
		private final UnqualifiedName originalName;
		private SQLCommand sql;
		private final List<CatalogEntity> updates;
		private final Map<UnqualifiedName,LateFKFixup> lateFKFixups;
		private boolean haveLock = false;
		private UnqualifiedName generatedName = null;
		
		public CreateTableOperation(SchemaContext sc, PETable def, UnqualifiedName logicalName, PETenant requestingTenant, TableState state,
				Map<UnqualifiedName,LateFKFixup> fixups) {
			this.definition = def;
			this.originalName = logicalName;
			this.updates = new ArrayList<CatalogEntity>();
			this.originator = requestingTenant;
			this.requestedState = state;
			this.lateFKFixups = fixups;
			if (fixups != null && state == TableState.SHARED) {
				for(PEKey pek : def.getKeys(sc)) {
					if (!pek.isForeign()) continue;
					PEForeignKey pefk = (PEForeignKey) pek;
					LateFKFixup any = fixups.get(pefk.getSymbol());
					if (any != null) {
						if (any.getTargetTable() == null)
							throw new IllegalStateException("Creating shared table " + originalName + " with dangling fixup");
						else if (!any.getTargetTable().isWellFormed())
							throw new IllegalStateException("Creating shared table " + originalName + " with fixed fixup");
					}
				}
			}
				
			targetTable = null;
		}

		public UnqualifiedName getLogicalName() {
			return originalName;
		}
		
		public PETable getDefinition() {
			return definition;
		}
		
		public TableState getState() {
			return requestedState;
		}
		
		@Override
		public PETable getTable(SchemaContext sc) {
			return sc.getSource().find(sc, targetTable).asTable();
		}	
		
		private PETable recreateTable(SchemaContext sc) {
			sc.setOptions(ParserOptions.NONE.setAllowTenantColumn().disableMTLookupChecks());
			PETable rc = definition.recreate(sc, definition.getDeclaration(), new LockInfo(LockType.EXCLUSIVE, "adaptive mt ddl (fks)"));
			if (!lateFKFixups.isEmpty()) {
				boolean changed = false;
				for(PEKey pek : rc.getKeys(sc)) {
					if (!pek.isForeign()) continue;
					PEForeignKey pefk = (PEForeignKey) pek;
					LateFKFixup any = lateFKFixups.get(pefk.getSymbol());
					if (any != null) {
						any.fixupForeignKey(sc, pefk);
						changed = true;
					}
				}
				if (changed) {
					rc.setDeclaration(sc, rc);
				}
			}
			return rc;
		}
		
		@Override
		public void beforeTxn(SSConnection ssCon, CatalogDAO c, WorkerGroup wg) throws PEException {
			updates.clear();
			targetTable = null;
			if (!haveLock) {
				SchemaContext sc = SchemaContext.createContext(ssCon);
				sc.forceMutableSource();
				PETable theTable = recreateTable(sc);
				PEDatabase pdb = theTable.getPEDatabase(sc);
				UserDatabase ofdb = pdb.getPersistent(sc);
				
				String shapeName = originalName.get();
				String hash = theTable.getTypeHash();
				
				Shape s = c.findShape(ofdb, shapeName, theTable.getDefinition(), hash);
				
				// take a shape lock here; if the shape exists we can use a shared lock.
				// otherwise it must be exclusive to avoid dup shapes in the catalog.
				ShapeLock theShapeLock = new ShapeLock("create tenant table " + originalName + " on tenant " + originator.getName(),sc,originalName.getUnqualified().getUnquotedName().get(),theTable);
				ssCon.acquireLock(theShapeLock, (s == null ? LockType.EXCLUSIVE : LockType.WSHARED));
				haveLock = true;
			}
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			CatalogDAO c = conn.getCatalogDAO();
			SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();
	
			PETable nt = recreateTable(sc); 
			
			PEDatabase pdb = nt.getPEDatabase(sc);
			UserDatabase ofdb = pdb.getPersistent(sc);
			
			String shapeName = originalName.get();
			String hash = nt.getTypeHash();
			
			Shape s = c.findShape(ofdb, shapeName, nt.getDefinition(), hash);
			if (s != null) {
				if (requestedState == TableState.SHARED) {
					c.refreshForLock(s);
					List<UserTable> candidates = c.findMatchingTables(s,TableState.SHARED);
					UserTable existingSharedTable = (candidates.isEmpty() ? null : candidates.get(0));
					if (existingSharedTable != null)
						targetTable = (TableCacheKey) PETable.getTableKey(existingSharedTable);
				}
			} else {
				c.refreshForLock(ofdb);
				s = c.findShape(ofdb, shapeName, nt.getDefinition(), hash);
				if (s == null) {
					s = new Shape(ofdb, shapeName, nt.getDefinition(), hash);
					updates.add(s);
				}
			}

			if (targetTable == null) {
				nt.setState(requestedState);
				if (generatedName == null) {
					PETenant reloaded = (PETenant) sc.getSource().find(sc, originator.getCacheKey());
					generatedName = reloaded.buildPrivateTableName(sc, originalName);
				}
				nt.resetName(sc, generatedName);
				nt.setDeclaration(sc, nt);				
				if (requestedState == TableState.SHARED) {
					for(PEKey pek : nt.getKeys(sc)) {
						if (!pek.isForeign()) continue;
						PEForeignKey pefk = (PEForeignKey) pek;
						if (pefk.isForward())
							throw new IllegalStateException("Creating shared table " + generatedName + " with dangling fk " + pefk.getSymbol());
						PETable targ = pefk.getTargetTable(sc);
						if (targ.getState() == TableState.FIXED)
							throw new IllegalStateException("Creating shared table " + generatedName + " with fk " + pefk.getSymbol() + " targeting fixed table " + targ.getName());
					}
				}
				sc.beginSaveContext();
				UserTable thePersistentTable = null;
				try {
					thePersistentTable = nt.persistTree(sc);
					updates.addAll(sc.getSaveContext().getObjects());
				} finally {
					sc.endSaveContext();
				}
				thePersistentTable.setShape(s);
				sql = new SQLCommand(nt.getDeclaration());
				targetTable = (TableCacheKey) PETable.getTableKey(pdb, generatedName);
			} else {
				sql = SQLCommand.EMPTY;
			}				
		}

		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updates;
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return (sql == null ? SQLCommand.EMPTY : sql);
		}

		@Override
		public boolean canRetry(Throwable t) {
			return AdaptiveMultitenantSchemaPolicyContext.canRetry(t);
		}

		@Override
		public boolean requiresFreshTxn() {
			return true;
		}

		@Override
		public String description() {
			return this.getClass().getSimpleName() + " for table named " + definition.getName();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			// new table, no invalidation record
			return null;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

		@Override
		public String describe() {
			return description();
		}

		@Override
		public void postCommitAction(CatalogDAO c) throws PEException {
			CatalogSanity.assertCatalogSanity(c);
		}

		@Override
		public boolean isWellFormed() {
			return requestedState == TableState.SHARED;
		}

	}

	public interface ChangeSource extends LazyAllocatedTable {
		
		public List<QueryStep> getDDLSteps();
		
		public List<QueryStep>  getDMLSteps();
		
		public void beforeTxn(SSConnection ssConn);
		
		public void inTxn(SchemaContext sc, CatalogDAO c) throws PEException;
		
		public CacheInvalidationRecord getInvalidationRecord();
		
		public List<CatalogEntity> getUpdates();
		
		public List<CatalogEntity> getDeletes();
		
		public boolean requiresWorkers();
		
		public QueryStep getCanonicalResultStep();

	}
	
	
	public static class CompositeNestedOperation extends NestedOperationDDLCallback {
		
		protected final List<ChangeSource> changes;
		
		protected List<CatalogEntity> updates;
		protected List<CatalogEntity> deletes;
		
		protected List<QueryStep> steps;
		
		protected QueryStep canonicalResultStep;
		
		protected CacheInvalidationRecord record;
		
		public CompositeNestedOperation() {
			this.changes = new ArrayList<ChangeSource>();
		}
		
		public CompositeNestedOperation withChange(ChangeSource cs) {
			this.changes.add(cs);
			return this;
		}
		
		public CompositeNestedOperation withChanges(List<ChangeSource> multi) {
			this.changes.addAll(multi);
			return this;
		}
		
		@Override
		public void beforeTxn(SSConnection ssCon, CatalogDAO c, WorkerGroup wg) throws PEException {
			for(ChangeSource cs : changes)
				cs.beforeTxn(ssCon);
			updates = null;
			deletes = null;
			steps = null;
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			CatalogDAO c = conn.getCatalogDAO();
			SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();

			for(ChangeSource cs : changes)
				cs.inTxn(sc,c);

			// for both updates and deletes we are going to preserve the order they come off the changes
			LinkedHashSet<CatalogEntity> nodupes = new LinkedHashSet<CatalogEntity>();
			for(ChangeSource cs : changes)
				nodupes.addAll(cs.getUpdates());
			updates = Functional.toList(nodupes);
			nodupes.clear();
			for(ChangeSource cs : changes)
				nodupes.addAll(cs.getDeletes());
			deletes = Functional.toList(nodupes);
			steps = new ArrayList<QueryStep>();
			for(ChangeSource cs : changes) 
				accumulateSteps(cs,cs.getDDLSteps());
			for(ChangeSource cs : changes) 
				accumulateSteps(cs,cs.getDMLSteps());
		}

		private void accumulateSteps(ChangeSource cs, List<QueryStep> t) {
			if (cs.getCanonicalResultStep() != null)
				canonicalResultStep = cs.getCanonicalResultStep();
			steps.addAll(t);
		}
		
		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			return deletes;
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
			StringBuilder buf = new StringBuilder();
			buf.append("CompositeNestedOperation{");
			buf.append(Functional.join(changes, ",", new UnaryFunction<String,ChangeSource>() {

				@Override
				public String evaluate(ChangeSource object) {
					return object.describe();
				}
				
			}));
			buf.append("}");
			return buf.toString();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			if (record == null) {
				ListOfPairs<SchemaCacheKey<?>,InvalidationScope> all = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
				for(ChangeSource cs : changes) {
					CacheInvalidationRecord sub = cs.getInvalidationRecord();
					if (sub == null) continue;
					all.addAll(sub.getInvalidateActions());
				}
				record = new CacheInvalidationRecord(all);
			}
			return record;
		}

		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg,
				DBResultConsumer resultConsumer) throws Throwable {
			for(QueryStep qs : steps) {
				QueryStepOperation qso = qs.getOperation();
				boolean care = (qs == canonicalResultStep || (canonicalResultStep == null && qs == steps.get(steps.size() - 1)));
				qso.execute(conn, wg, (care ? resultConsumer : DBEmptyTextResultConsumer.INSTANCE));
			}
		}

		@Override
		public boolean requiresWorkers() {
			for (ChangeSource cs : changes)
				if (cs.requiresWorkers())
					return true;
			return false;
		}

		@Override
		public void postCommitAction(CatalogDAO c) throws PEException {
			try {
				CatalogSanity.assertCatalogSanity(c);
			} catch (PEException pe) {
				throw new PEException(description(),pe);
			}
		}
	}
	
	public static class TableFlip implements ChangeSource {

		private final TableScope scope;
		private final TableCacheKey srcTable;
		private final LazyAllocatedTable targetTable; 
		private final List<CatalogEntity> updates;
		private final List<QueryStep> steps;
		private final CacheInvalidationRecord record;
		private final boolean isCanonical;
		private QueryStep canonicalStep;
		
		public TableFlip(TableScope theScope, TableCacheKey theSrcTab, LazyAllocatedTable theTargetTab,
				boolean canonicalResults,
				CacheInvalidationRecord cacheClear) {
			this.scope = theScope;
			this.srcTable = theSrcTab;
			this.targetTable = theTargetTab;
			this.updates = new ArrayList<CatalogEntity>();
			this.steps = new ArrayList<QueryStep>();
			this.record = cacheClear;
			this.isCanonical = canonicalResults;
		}

		@Override
		public PETable getTable(SchemaContext sc) {
			return targetTable.getTable(sc);
		}
		

		@Override
		public List<QueryStep> getDDLSteps() {
			return Collections.emptyList();
		}

		@Override
		public List<QueryStep> getDMLSteps() {
			return steps;
		}

		@Override
		public void beforeTxn(SSConnection ssCon) {
			steps.clear();
			updates.clear();
		}

		@Override
		public void inTxn(SchemaContext sc, CatalogDAO c) throws PEException {
			PETable srcTab = sc.getSource().find(sc, srcTable).asTable();
			PETable targetTab = targetTable.getTable(sc);
			
			steps.addAll(buildSteps(sc,scope,srcTab,targetTab));

			TableVisibility tv = scope.getPersistent(sc);
			UserTable targetUserTable = targetTab.getPersistent(sc);
			tv.setTable(targetUserTable);
			
			updates.add(tv);
            Singletons.require(HostService.class).onGarbageEvent();
		}

		private List<QueryStep> buildSteps(SchemaContext sc, TableScope scope, PETable currentTable, PETable newTable) throws PEException {
			ExecutionSequence es = new ExecutionSequence(null);
			
			Map<PEColumn, PEColumn> columnMap = buildForwarding(sc, currentTable,newTable);

			// allocate a new table instance
			TableKey targtk = new MTTableKey(newTable,scope,sc.getNextTable());
			TableInstance targti = targtk.toInstance();
			List<ExpressionNode> targColumns = getColumns(sc, targti, columnMap);
			
			if (newTable.getColumns(sc).size() > currentTable.getColumns(sc).size()
					&& targColumns.size() < newTable.getColumns(sc).size())
				throw new PEException("Missing constant value in src select");
			
			// build a select against the source table for the source tenant
			SelectStatement srcSelect = buildSourceSelect(sc,scope,currentTable,targColumns,columnMap);
			// apply the mt filter in order to set the correct tenant id on it - not the current tenant id
			AdaptiveMultitenantSchemaPolicyContext mspc = (AdaptiveMultitenantSchemaPolicyContext) sc.getPolicyContext();
			mspc.applyDegenerateMultitenantFilter(srcSelect, LiteralExpression.makeAutoIncrLiteral(scope.getTenant(sc).getTenantID()));
			InsertIntoSelectStatement iiss = 
				new InsertIntoSelectStatement(targti,targColumns,srcSelect,false,null,(AliasInformation)null,null);
			iiss.plan(sc, es, sc.getBehaviorConfiguration());
			
			DeleteStatement ds = AdaptiveMultitenantSchemaPolicyContext.buildTenantDeleteFromTableStatement(sc, currentTable, scope);
			ds.plan(sc,es, sc.getBehaviorConfiguration());
			
			List<QueryStep> allsteps = new ArrayList<QueryStep>();
			es.schedule(null, allsteps, null, sc);
			if (isCanonical)
				canonicalStep = allsteps.get(0);
			return allsteps;

		}
		
		// target may have more columns, or fewer columns, than src.  fill in the appropriate columns
		// with the default expression
		private SelectStatement buildSourceSelect(SchemaContext pc, TableScope scope, PETable srctab, List<ExpressionNode> matchColumns, 
				Map<PEColumn,PEColumn> forwarding) {
			TableKey srctk = new MTTableKey(srctab,scope,pc.getNextTable());
			TableInstance ti = srctk.toInstance();
			List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
			AliasInformation ai = new AliasInformation();
			for(ExpressionNode te : matchColumns) {
				ColumnInstance tci = (ColumnInstance)te;
				PEColumn tc = tci.getPEColumn();
				PEColumn sc = forwarding.get(tc);
				ExpressionNode value = null;
				if (sc != null) {
					value = new ColumnInstance(sc,ti);
				} else if (!tc.isAutoIncrement()) {
					if (tc.isNullable())
						value = LiteralExpression.makeNullLiteral();
					else 
						value = tc.getType().getZeroValueLiteral();
				}
				if (value != null) {
					ExpressionAlias ea = new ExpressionAlias(value,new NameAlias(tc.getName().getUnqualified()),false);
					ai.addAlias(tc.getName().getUnqualified().getUnquotedName().get());
					proj.add(ea);
				} else {
				
				}
			}
			SelectStatement ss = new SelectStatement(ai)
				.setTables(ti)
				.setProjection(proj)
				.setLocking();
			ss.getDerivedInfo().addLocalTable(ti.getTableKey());
			ss.normalize(pc);
			return ss;
		}

		private List<ExpressionNode> getColumns(SchemaContext pc, final TableInstance ti, Map<PEColumn, PEColumn> forwarding) {
			PETable pet = (PETable) ti.getTable();
			List<ExpressionNode> out = new ArrayList<ExpressionNode>();
			for(PEColumn tc : pet.getColumns(pc)) {
				PEColumn sc = forwarding.get(tc);
				if (sc != null || !tc.isAutoIncrement()) {
					out.add(new ColumnInstance(tc,ti));
				}
			}
			return out;
		}

		// build a map from target columns to source columns
		private Map<PEColumn, PEColumn> buildForwarding(SchemaContext pc, PETable src, PETable target) {
			// figure out the column forwarding - 4 cases
			// [1] column is removed
			// [2] column is added
			// [3] column is renamed
			// [4] column is changed but not renamed
			// first two - number of columns between src and target differ
			// last two - same - can do a direct map as column definition order would not change
			HashMap<PEColumn, PEColumn>  out = new HashMap<PEColumn, PEColumn>();
			List<PEColumn> srccols = src.getColumns(pc);
			List<PEColumn> targcols = target.getColumns(pc);
			if (srccols.size() == targcols.size()) {
				for(int i = 0; i < srccols.size(); i++) {
					out.put(targcols.get(i),srccols.get(i));
				}
				return out;
			} else {
				for(PEColumn tc : targcols) {
					PEColumn sc = src.lookup(pc,tc.getName());
					out.put(tc, sc);
				}
				return out;
			}
		}


		
		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public List<CatalogEntity> getUpdates() {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletes() {
			return Collections.emptyList();
		}

		@Override
		public boolean requiresWorkers() {
			// most assuredly
			return true;
		}

		@Override
		public QueryStep getCanonicalResultStep() {
			return canonicalStep;
		}

		@Override
		public String describe() {
			return "TableFlip from " + srcTable + " on scope " + scope.getCacheKey() + " to " + targetTable.describe();
		}

		@Override
		public boolean isWellFormed() {
			return targetTable.isWellFormed();
		}

	}

	public static class CreateTenantScope implements ChangeSource {

		private final List<CatalogEntity> updates;
		private final TenantCacheKey tenantKey;
		private final UnqualifiedName logicalName;
		private final Long autoIncOffset;
		private final LazyAllocatedTable backingTableName;
		
		public CreateTenantScope(TenantCacheKey tenant, UnqualifiedName visibleName, 	
				Long anyAutoInc, LazyAllocatedTable backingTableName) {
			this.tenantKey = tenant;
			this.logicalName = visibleName;
			this.autoIncOffset = anyAutoInc;
			this.backingTableName = backingTableName;
			this.updates = new ArrayList<CatalogEntity>();
		}
		
		@Override
		public List<QueryStep> getDDLSteps() {
			return Collections.emptyList();
		}

		@Override
		public List<QueryStep> getDMLSteps() {
			return Collections.emptyList();
		}

		@Override
		public void beforeTxn(SSConnection ssCon) {
			updates.clear();
		}

		@Override
		public void inTxn(SchemaContext sc, CatalogDAO c) throws PEException {
			
			PETable backing = backingTableName.getTable(sc);
			
			PETenant tenant = sc.getSource().find(sc, tenantKey);
			
			TableScope ts = tenant.setVisible(sc, backing, logicalName, autoIncOffset, new LockInfo(LockType.EXCLUSIVE,"mtddl"));

			sc.beginSaveContext();
			try {
				ts.persistTree(sc);
				updates.addAll(sc.getSaveContext().getObjects());
			} finally {
				sc.endSaveContext();
			}
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return null;
		}

		@Override
		public List<CatalogEntity> getUpdates() {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletes() {
			return Collections.emptyList();
		}

		@Override
		public boolean requiresWorkers() {
			return false;
		}

		@Override
		public PETable getTable(SchemaContext sc) {
			return backingTableName.getTable(sc);
		}

		@Override
		public QueryStep getCanonicalResultStep() {
			return null;
		}

		@Override
		public String describe() {
			return "CreateTenantScope " + logicalName + " on " + tenantKey;
		}

		@Override
		public boolean isWellFormed() {
			return backingTableName.isWellFormed();
		}
		
	}
	
	public static class DropTenantScope implements ChangeSource {

		private final ScopeCacheKey toDrop;
		private final List<CatalogEntity> drops;
		private final List<QueryStep> dml;
		private final CacheInvalidationRecord record;
		
		public DropTenantScope(ScopeCacheKey scope) {
			toDrop = scope;
			drops = new ArrayList<CatalogEntity>();
			dml = new ArrayList<QueryStep>();
			record = new CacheInvalidationRecord(scope.getTenantCacheKey(),InvalidationScope.CASCADE);
		}
		
		@Override
		public PETable getTable(SchemaContext sc) {
			return null;
		}

		@Override
		public List<QueryStep> getDDLSteps() {
			return Collections.emptyList();
		}

		@Override
		public List<QueryStep> getDMLSteps() {
			return dml;
		}

		@Override
		public void beforeTxn(SSConnection ssCon) {
			drops.clear();
			dml.clear();
		}

		@Override
		public void inTxn(SchemaContext sc, CatalogDAO c) throws PEException {
			TableScope actualScope = sc.getSource().find(sc, toDrop); 

			ExecutionSequence es = new ExecutionSequence(null);
			DeleteStatement ds = AdaptiveMultitenantSchemaPolicyContext.buildTenantDeleteFromTableStatement(sc, actualScope.getTable(sc), actualScope);
			ds.plan(sc,es, sc.getBehaviorConfiguration());
			es.schedule(null, dml, null, sc);
			
			sc.beginSaveContext();
			try {
				drops.add(actualScope.getPersistent(sc));
			} finally {
				sc.endSaveContext();
			}

            Singletons.require(HostService.class).onGarbageEvent();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public List<CatalogEntity> getUpdates() {
			return Collections.emptyList();
		}

		@Override
		public List<CatalogEntity> getDeletes() {
			return drops;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

		@Override
		public QueryStep getCanonicalResultStep() {
			return null;
		}

		@Override
		public String describe() {
			return "DropTenantScope on " + toDrop;
		}

		@Override
		public boolean isWellFormed() {
			return false;
		}
		
	}
	
	public static class UpdateForeignKeyTargetTable implements ChangeSource {

		private final LazyAllocatedTable newTargetTable;
		private final UnqualifiedName revertToForward;
		private final TableCacheKey currentTable;
		private final UnqualifiedName constraintName;
		private final List<CatalogEntity> updates;
		private final List<QueryStep> steps;
		private final CacheInvalidationRecord record;
		
		public UpdateForeignKeyTargetTable(TableCacheKey enclosing, UnqualifiedName constraintName, 
				LazyAllocatedTable theNewTarget, CacheInvalidationRecord record) {
			this(enclosing,constraintName,theNewTarget,null,record);
		}
		
		public UpdateForeignKeyTargetTable(TableCacheKey enclosing, UnqualifiedName constraintName,
				UnqualifiedName revertToForward, CacheInvalidationRecord record) {
			this(enclosing,constraintName,null,revertToForward,record);
		}
		
		private UpdateForeignKeyTargetTable(TableCacheKey enclosing, UnqualifiedName constraintName, 
				LazyAllocatedTable theNewTarget, UnqualifiedName revertToForward,
				CacheInvalidationRecord record) {
			this.newTargetTable = theNewTarget;
			this.revertToForward = revertToForward;
			this.updates = new ArrayList<CatalogEntity>();
			this.steps = new ArrayList<QueryStep>();
			this.record = record;
			this.currentTable = enclosing;
			this.constraintName = constraintName;
		}
		
		
		@Override
		public PETable getTable(SchemaContext sc) {
			return newTargetTable.getTable(sc);
		}

		@Override
		public List<QueryStep> getDDLSteps() {
			return steps;
		}

		@Override
		public List<QueryStep> getDMLSteps() {
			return Collections.emptyList();
		}

		@Override
		public void beforeTxn(SSConnection ssConn) {
			steps.clear();
			updates.clear();
		}

		@Override
		public void inTxn(SchemaContext sc, CatalogDAO c) throws PEException {
			PETable enclosing = sc.getSource().find(sc, currentTable).asTable(); 
			sc.beginSaveContext();
			try {
				enclosing.persistTree(sc);
			} finally {
				sc.endSaveContext();
			}
			PEForeignKey constraint = null;
			for(PEKey pek : enclosing.getKeys(sc)) {
				if (!pek.isForeign()) continue;
				PEForeignKey pefk = (PEForeignKey) pek;
				if (pefk.getSymbol().equals(constraintName)) {
					constraint = pefk;
					break;
				}
			}
			
			TableKey yonKey = new TableKey(enclosing,sc.getNextTable());
			AlterTableAction dropCurrentDefinition = 
					new DropIndexAction(constraint);
			PEAlterTableStatement dropOld = 
					new PEAlterTableStatement(sc, yonKey,dropCurrentDefinition);
			String dropCurrentSQL = dropOld.getSQL(sc);
			
			if (revertToForward == null) {
				PETable newTarget = newTargetTable.getTable(sc);
				for(PEKeyColumnBase pekc : constraint.getKeyColumns()) {
					PEForeignKeyColumn pefkc = (PEForeignKeyColumn) pekc;
					pefkc.setTargetColumn(newTarget.lookup(sc, pefkc.getTargetColumnName()));
				}
				constraint.setTargetTable(sc, newTarget);
			} else {
				constraint.revertToForwardMT(sc, revertToForward);
			}
			
			AlterTableAction addCurrentDefinition =
					new AddIndexAction(constraint);
			PEAlterTableStatement addNew =
					new PEAlterTableStatement(sc, yonKey,addCurrentDefinition);
			String addNewSQL = addNew.getSQL(sc);
			
			enclosing.setDeclaration(sc, enclosing);
			
			if (AdaptiveMTDDLPlannerUtils.isWellFormed(sc, enclosing, null) && enclosing.getState() == TableState.FIXED) {
				throw new IllegalStateException("Updated foreign key " + constraint.getSymbol() + ", table " + enclosing.getName() + " is now well formed but is not being flipped");
			}
			
			sc.beginSaveContext();
			try {
				enclosing.persistTree(sc);
				updates.addAll(sc.getSaveContext().getObjects());
			} finally {
				sc.endSaveContext();
			}

			Key k = constraint.getPersistent(sc);
			if (revertToForward == null) {
				UserTable nut = newTargetTable.getTable(sc).getPersistent(sc);
				k.setReferencedTable(nut);
				for(PEKeyColumnBase pekc : constraint.getKeyColumns()) {
					PEForeignKeyColumn pefkc = (PEForeignKeyColumn) pekc;
					KeyColumn kc = pefkc.getPersistent(sc);
					kc.setTargetColumn(nut.getUserColumn(pefkc.getTargetColumnName().getUnquotedName().get()));
				}					
			}

			SessionExecutionStep drop = 
					new SessionExecutionStep(enclosing.getDatabase(sc),enclosing.getStorageGroup(sc),dropCurrentSQL);
			SessionExecutionStep add =
					new SessionExecutionStep(enclosing.getDatabase(sc),enclosing.getStorageGroup(sc),addNewSQL);
			drop.schedule(null, steps, null, sc);
			add.schedule(null, steps, null, sc);
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public List<CatalogEntity> getUpdates() {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletes() {
			return Collections.emptyList();
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

		@Override
		public QueryStep getCanonicalResultStep() {
			return null;
		}

		@Override
		public String describe() {
			return "UpdateForeignKeyTargetTable on " + currentTable + ", symbol " + constraintName;
		}

		@Override
		public boolean isWellFormed() {
			if (newTargetTable != null)
				return newTargetTable.isWellFormed();
			return false;
		}
		
	}
	
	public static class ExecuteAlters implements ChangeSource {

		private final PEAlterTableStatement original;
		private final LazyAllocatedTable target;
		private final List<CatalogEntity> updates;
		private final List<CatalogEntity> deletes;
		private final List<QueryStep> ddl;
		private PETable finalDefinition;
		private final boolean isCanonical;
		private QueryStep canonicalStep;
		
		public ExecuteAlters(PEAlterTableStatement src, boolean canonical, LazyAllocatedTable target) {
			this.original = src;
			this.target = target;
			this.updates = new ArrayList<CatalogEntity>();
			this.deletes = new ArrayList<CatalogEntity>();
			this.ddl = new ArrayList<QueryStep>();
			this.isCanonical = canonical;
		}
		
		@Override
		public PETable getTable(SchemaContext sc) {
			return finalDefinition;
		}

		@Override
		public List<QueryStep> getDDLSteps() {
			return ddl;
		}

		@Override
		public List<QueryStep> getDMLSteps() {
			return Collections.emptyList();
		}

		@Override
		public void beforeTxn(SSConnection ssConn) {
			ddl.clear();
			updates.clear();
			deletes.clear();
		}

		@Override
		public void inTxn(SchemaContext sc, CatalogDAO c) throws PEException {
			
			finalDefinition = target.getTable(sc); 
			
			if (finalDefinition.getState() == TableState.SHARED) {
				throw new IllegalStateException("Altering shared table " + finalDefinition.getName());
			}
			
			List<AlterTableAction> actions = original.getActions();
			TableKey tk = new TableKey(finalDefinition,sc.getNextTable());
			PEAlterTableStatement modded = new PEAlterTableStatement(sc,tk,actions);

			updates.addAll(modded.getCatalogEntries(sc));
			deletes.addAll(modded.getDeleteObjects(sc));
			
			SessionExecutionStep ses = new SessionExecutionStep(finalDefinition.getPEDatabase(sc),finalDefinition.getStorageGroup(sc),
					modded.getSQL(sc));
			ses.schedule(null, ddl, null, sc);
			if (isCanonical)
				canonicalStep = ddl.get(0);
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return null;
		}

		@Override
		public List<CatalogEntity> getUpdates() {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletes() {
			return deletes;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

		@Override
		public QueryStep getCanonicalResultStep() {
			return canonicalStep;
		}

		@Override
		public String describe() {
			return "ExecuteAlters";
		}

		@Override
		public boolean isWellFormed() {
			return target.isWellFormed();
		}
		
		
	}

	// observe that once we do whatever we're supposed to do to
	// the nearest neighbors all the ddl acts the same - i.e.
	// currently well formed	well formed with changes		action
	// yes						yes								flip to shared
	// yes						no								flip to fixed
	// no						yes								flip to shared
	// no						no								alter in place

	public static class LateFKFixup {
		
		private final LazyAllocatedTable forwardToTable;
		private final Name revertToForwardTable;
		private final UnqualifiedName symbolName;
		private final String reason;
		
		public LateFKFixup(UnqualifiedName constraintSymbol, LazyAllocatedTable newTable, String reason) {
			this.symbolName = constraintSymbol;
			this.revertToForwardTable = null;
			this.forwardToTable = newTable;
			this.reason = reason;
		}
		
		public LateFKFixup(UnqualifiedName constraintSymbol, Name revertTo, String reason) {
			this.symbolName = constraintSymbol;
			this.revertToForwardTable = revertTo;
			this.forwardToTable = null;
			this.reason = reason;
		}
		
		public String getReason() {
			return reason;
		}
		
		public UnqualifiedName getSymbolName() {
			return symbolName;
		}
		
		public String getTargetName() {
			if (forwardToTable != null)
				return forwardToTable.describe();
			return revertToForwardTable.get();
		}
		
		public LazyAllocatedTable getTargetTable() {
			return forwardToTable;
		}
		
		public void fixupForeignKey(SchemaContext sc, PEForeignKey pefk) {
			if (revertToForwardTable != null) {
				pefk.revertToForwardMT(sc, revertToForwardTable);
			} else {
				pefk.setTargetTable(sc, forwardToTable.getTable(sc));
			}			
		}
		
	}
	
	
	// well formedness.  the definition of a well formed table is:
	// [1] a table that has no foreign keys
	// [2] or a table that has no forward foreign keys and only references shared tables
	// 
	// the other issue is we modify the definitions in memory - so when computing well formedness we need the ability
	// to pass in a mapping of old to new tables - and the new tables will then have a state associated with them.
	public static boolean isWellFormed(SchemaContext sc, PETable table, Map<UnqualifiedName, LateFKFixup> symbolForwarding) {
		for(PEKey pek : table.getKeys(sc)) {
			if (!pek.isForeign()) continue;
			PEForeignKey pefk = (PEForeignKey) pek;
			// first see if the symbol has been forwarded
			LateFKFixup fixed = (symbolForwarding == null ? null : symbolForwarding.get(pefk.getSymbol()));
			if (fixed != null) {
				if (fixed.getTargetTable() == null) {
					// been reverted to forward
					return false;
				} else if (!fixed.getTargetTable().isWellFormed()) {
					// been forwarded to something fixed
					return false;
				} else {
					continue;
				}
			}
			// hasn't been forwarded yet, use the local checks
			if (pefk.isForward()) return false;
			PETable targ = pefk.getTargetTable(sc);
			if (targ.getState() == TableState.FIXED) return false;
		}
		return true;
	}
}
