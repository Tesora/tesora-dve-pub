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
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.queryplan.ExecutionState;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.UnresolvedRangeDistributionVector;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.modifiers.AutoincTableModifier;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.RenameTableAction;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.IdentityConnectionValuesMap;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.worker.WorkerGroup;

// we use a multistep plan for this.  the steps are:
// create a mangled name based on the existing table name, call this newmang
// create a new table like old table named newmang
// insert into newmang select * from table for update
// drop table
// rename newmang to table name
//
// of course, if the new dist vect is the same as the old dist vect, do nothing
// also, eventually we will need to take a lock - probably before the first create
// 
// for now - if there are fks referencing this table - don't allow the alter
// likewise if this table has fks - don't allow the alter
// we can relax this restriction later
public class AlterTableDistributionStatement extends PEAlterStatement<PETable> {

	CacheInvalidationRecord record;
	DistributionVector nv;
	
	public AlterTableDistributionStatement(SchemaContext sc, PETable target,
			DistributionVector inv) {
		super(target, true);
		record = new CacheInvalidationRecord(target.getCacheKey(),InvalidationScope.CASCADE);
		if (inv instanceof UnresolvedRangeDistributionVector) {
			UnresolvedRangeDistributionVector unres = (UnresolvedRangeDistributionVector) inv;
			inv = unres.resolve(sc, target.getPersistentStorage(sc));
		}
		this.nv = inv;
	}

	public DistributionVector getNewVector() {
		return nv;
	}
	
	@Override
	protected PETable modify(SchemaContext sc, PETable backing) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return record;
	}

	@Override
	public void normalize(SchemaContext sc) {
		super.normalize(sc);
		// check for fks within target, also fks referencing target - we disallow this for now
		PETable targ = getTarget();
		if (!targ.getForeignKeys(sc).isEmpty()) {
			throw new SchemaException(Pass.NORMALIZE, "No current support for altering distribution of table with foreign keys");
		}
		if (!targ.getReferencingTables().isEmpty()) {
			throw new SchemaException(Pass.NORMALIZE, "No current support for altering distribution of table with referring foreign keys");
		}
		if (nv.isContainer())
			// we mostly can't do this unless there is a tenant on the connection
			throw new SchemaException(Pass.NORMALIZE, "No current support for altering a table into a container");
		if (targ.getDistributionVector(sc).isContainer())
			throw new SchemaException(Pass.NORMALIZE, "No current support for altering the distribution of a table in a container");
	}

	
	// we have our own stmt because we have nontrivial planning involved
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		normalize(sc);
		// the dist vect is already in the appropriate context, let's get to work
		// only one ddl step can actually be executed using the current  
		PETable oldTab = getTarget();
		TableKey oldTableKey = TableKey.make(sc, oldTab,sc.getNextTable());
		long now = System.currentTimeMillis();
		String mangledNewName = oldTab.getName().getUnquotedName().get() + now;
		PETable newTab = oldTab.recreate(sc, oldTab.getDeclaration(), new LockInfo(LockType.EXCLUSIVE, "alter distribution"));
		newTab.setName(new UnqualifiedName(mangledNewName));
		DistributionVector adv = nv.adapt(sc, newTab);
		newTab.setDistributionVector(sc, adv);
		if (oldTab.hasAutoInc()) {
			// figure out the autoinc value, copy it over
			long nextVal = oldTableKey.readAutoIncrBlock(sc); 
			if (nextVal > 1)
				newTab.getModifiers().setModifier(new AutoincTableModifier(nextVal));
		}
		newTab.setDeclaration(sc, newTab);
		TableKey newTableKey = TableKey.make(sc,newTab,sc.getNextTable());
		// new table in hand, I can create it
		PECreateTableStatement createNew = new PECreateTableStatement(newTab,false);
		createNew.plan(sc,es, config);
		// a callback here to do the actual copy
		es.append(new ComplexDDLExecutionStep(oldTab.getPEDatabase(sc),oldTab.getStorageGroup(sc),
				null,Action.ALTER,new CopyTable((TableCacheKey)oldTab.getCacheKey(),
						(TableCacheKey)PETable.getTableKey(oldTab.getPEDatabase(sc), newTab.getName()))));
		@SuppressWarnings("unchecked")
		PEDropTableStatement dropTab = new PEDropTableStatement(sc,Collections.singletonList(oldTableKey),Collections.EMPTY_LIST,null,oldTableKey.isUserlandTemporaryTable());
		dropTab.plan(sc,es, config);
		AlterTableAction ata = new RenameTableAction(oldTab.getName(),false);
		PEAlterTableStatement alter = new PEAlterTableStatement(sc,newTableKey,Collections.singletonList(ata));
		es.append(new ComplexDDLExecutionStep(oldTab.getPEDatabase(sc),oldTab.getStorageGroup(sc),
				null,Action.ALTER,new RenameTable(sc,alter)));
	}
	
	private static class CopyTable extends NestedOperationDDLCallback {

		TableCacheKey otck;
		TableCacheKey ntck;
		List<QueryStepOperation> dml;
		CacheInvalidationRecord record;
		
		public CopyTable(TableCacheKey oldTable, TableCacheKey newTable) {
			otck = oldTable;
			ntck = newTable;
			ListOfPairs<SchemaCacheKey<?>,InvalidationScope> pairs = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
			pairs.add(otck,InvalidationScope.CASCADE);
			pairs.add(ntck,InvalidationScope.CASCADE);
			record = new CacheInvalidationRecord(pairs);
		}
		
		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			// just create a new schema context
			SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();
			// lookup both tables
			PETable oldTab = sc.getSource().find(sc, otck).asTable();
			PETable newTab = sc.getSource().find(sc, ntck).asTable();
			
			final TableInstance oti = new TableInstance(oldTab,oldTab.getName(),
					new UnqualifiedName("copyfrom"),sc.getNextTable(),true);
			final TableInstance nti = new TableInstance(newTab,newTab.getName(),
					new UnqualifiedName("copyto"),sc.getNextTable(),true);
			
			List<ExpressionNode> proj = Functional.apply(oldTab.getColumns(sc),new UnaryFunction<ExpressionNode,PEColumn>() {

				@Override
				public ExpressionNode evaluate(PEColumn object) {
					return new ColumnInstance(object,oti);
				}
				
			});
			List<ExpressionNode> nproj = Functional.apply(newTab.getColumns(sc), new UnaryFunction<ExpressionNode,PEColumn>() {

				@Override
				public ExpressionNode evaluate(PEColumn object) {
					return new ColumnInstance(object,nti);
				}
				
			});
			SelectStatement ss = new SelectStatement(new AliasInformation())
				.setTables(oti).setProjection(proj).setLocking();
			ss.getDerivedInfo().addLocalTable(oti.getTableKey());
			InsertIntoSelectStatement iiss = new InsertIntoSelectStatement(
					nti,
					nproj,ss,false,
					null,
					new AliasInformation(), 
					null);
			iiss.getDerivedInfo().addNestedStatements(Collections.singleton((ProjectingStatement)ss));
			iiss.getDerivedInfo().addLocalTable(nti.getTableKey());

			iiss.normalize(sc);
			ExecutionSequence es = new ExecutionSequence(null);
			iiss.plan(sc,es, sc.getBehaviorConfiguration());
			List<QueryStepOperation> steps = new ArrayList<QueryStepOperation>();
			es.schedule(null, steps, null, sc, new IdentityConnectionValuesMap(sc.getValues()),null);
			dml = steps;
		}

		@Override
		public boolean canRetry(Throwable t) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean requiresFreshTxn() {
			// I don't think we want to retry this  
			return false;
		}

		@Override
		public String description() {
			return "CopyTable@" + System.identityHashCode(this) + " from " + otck.toString() + " to " + ntck.toString();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public void executeNested(ExecutionState execState, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
			if (dml != null) {
				for (QueryStepOperation qso : dml) {
					qso.executeSelf(execState, wg, resultConsumer);
				}
			}
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}
	}
	
	private static class RenameTable extends DDLCallback {

		protected PEAlterTableStatement modifier;
		protected List<CatalogEntity> deletes = null;
		protected List<CatalogEntity> updates = null;
		protected SQLCommand sql = SQLCommand.EMPTY;
		protected final SchemaContext context;
		
		public RenameTable(SchemaContext sc, PEAlterTableStatement orig) {
			modifier = orig;
			context = sc;
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
		public SQLCommand getCommand(CatalogDAO c) {
			if (c == null)
				return modifier.getSQLCommand(context);
			else
				return sql;
		}

		@Override
		public void beforeTxn(SSConnection ssConn, CatalogDAO c, WorkerGroup wg) throws PEException {
			deletes = null;
			updates = null;
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			SchemaContext cntxt = SchemaContext.createContext(conn,context);
			cntxt.forceMutableSource();
			
			modifier.refresh(cntxt);

			sql = modifier.getSQLCommand(cntxt);

			updates = modifier.getCatalogObjects(cntxt);
			deletes = Collections.emptyList();
			
		}

		@Override
		public boolean canRetry(Throwable t) {
			return false;
		}

		@Override
		public String description() {
			return System.identityHashCode(this) + "@RenameTable: " + modifier.getSQL(context);
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return modifier.getInvalidationRecord(context);
		}

		@Override
		public boolean requiresFreshTxn() {
			return true;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

	}
}
