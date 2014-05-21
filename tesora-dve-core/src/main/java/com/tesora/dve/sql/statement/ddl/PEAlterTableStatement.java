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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.queryplan.QueryStepFilterOperation.OperationFilter;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction.ClonableAlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.ConvertToAction;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep;
import com.tesora.dve.sql.transform.execution.SessionExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.WorkerGroup;

// an alter statement can have one or more actions.  we either execute all of them or none of them.
public class PEAlterTableStatement extends PEAlterStatement<PETable> {
	
	TableKey targetTableKey;
	
	private List<AlterTableAction> actions;
	private List<AlterTableAction> finalActions;

	public PEAlterTableStatement(SchemaContext sc, TableKey target, List<AlterTableAction> alters) {
		super(target.getAbstractTable().asTable(), false);
		actions = alters;
		targetTableKey = target;
		finalActions = null;
		for(AlterTableAction ata : actions) {
			ata.setTarget(sc, target.getAbstractTable().asTable());
		}
	}

	public PEAlterTableStatement(SchemaContext sc, TableKey target, AlterTableAction singleAlter) {
		this(sc, target,Collections.singletonList(singleAlter));
	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext pc) {
		ListOfPairs<SchemaCacheKey<?>,InvalidationScope> clears = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
		clears.add(targetTableKey.getAbstractTable().getPEDatabase(pc)
				.getCacheKey(), InvalidationScope.CASCADE);
		return new CacheInvalidationRecord(clears);
	}
	
	public TableKey getTableKey() {
		return targetTableKey;
	}

	
	public void refresh(SchemaContext sc) {
		backing = (PETable) backing.reload(sc);
		for(AlterTableAction aa : actions)
			aa.refresh(sc, backing);
	}
	
	public List<AlterTableAction> getActions() {
		if (finalActions != null) 
			return finalActions;
		return actions;
	}
	
	public boolean hasSQL(SchemaContext pc) {
		for(AlterTableAction aa : actions) {
			if (aa.hasSQL(pc, backing))
				return true;
		}
		return false;
	}
	
	@Override
	protected PETable modify(SchemaContext pc, PETable in) {
		throw new SchemaException(Pass.PLANNER, "Invalid call to alter table statement.modify");
	}

	public boolean isNoop(SchemaContext pc, PETable pet) {
		for(AlterTableAction aa : actions) {
			if (!aa.isNoop(pc,pet))
				return false;
		}
		return true;
	}
	
	public String isValid(SchemaContext pc, PETable pet) {
		for(AlterTableAction aa : actions) {
			String v = aa.isValid(pc,pet);
			if (v != null) return v;
		}
		return null;
	}
	
	public PEAlterTableStatement adapt(SchemaContext pc, TableKey pet) {
		ArrayList<AlterTableAction> newacts = new ArrayList<AlterTableAction>();
		for(AlterTableAction aa : actions)
			newacts.add(aa.adapt(pc,pet.getAbstractTable().asTable()));
		return new PEAlterTableStatement(pc,pet,newacts);
	}
	
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		ArrayList<CatalogEntity> ret = new ArrayList<CatalogEntity>();
		List<AlterTableAction> use = (finalActions != null ? finalActions : actions);
		for(AlterTableAction aa : use)
			ret.addAll(aa.getDeleteObjects(pc));
		return ret;
	}

	private AlterTableAction applyOneAction(SchemaContext pc, AlterTableAction aa, PETable tschema, PETable pschema) {
		AlterTableAction add = aa.alterTable(pc,tschema);
		if (pschema != null)
			add = aa.alterTable(pc,pschema);
		return add;
	}
	
	public void alterTable(SchemaContext pc, PETable tschema, PETable pschema) {
		finalActions = new ArrayList<AlterTableAction>();
		for(AlterTableAction aa : actions) {
			AlterTableAction currentAction = aa;
			while(currentAction != null) {
				finalActions.add(currentAction);
				currentAction = applyOneAction(pc, currentAction,tschema,pschema);
			}
		}
		if (finalActions.size() == actions.size())
			finalActions = null;
	}
	
	// override this to utilize the various functions 
	@Override
	public List<CatalogEntity> getCatalogEntries(SchemaContext pc) throws PEException {
		pc.beginSaveContext();
		try {
		backing.persistTree(pc,true);		
		PETable tschemaVersion = buildTSchemaTable(pc,backing);
		// so the actions have to be done one at a time in case we have something like drop index, add index
		finalActions = new ArrayList<AlterTableAction>();		
		for(AlterTableAction aa : actions) {
			String v = aa.isValid(pc,backing);
			if (v != null)
				throw new PEException(v);
			v = aa.isValid(pc, tschemaVersion);
			if (v != null)
				throw new PEException(v);
			AlterTableAction currentAction = aa;
			while(currentAction != null) {
				finalActions.add(currentAction);
				currentAction = applyOneAction(pc, currentAction,tschemaVersion,backing);
			}
		}
		if (finalActions.size() == actions.size())
			finalActions = null;
		
		backing.setDeclaration(pc,tschemaVersion);
		} finally {
			pc.endSaveContext();
		}
		pc.beginSaveContext(true);
		try {
			backing.persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}
	
	// go back to the source, get a tschema version
	public PETable buildTSchemaTable(SchemaContext pc, PETable persTable) throws PEException {
		String cts = persTable.getDeclaration();
		if (cts == null)
			throw new PEException("No stored create table statement");
		return persTable.recreate(pc, cts, new LockInfo(LockType.EXCLUSIVE, "alter table"));
	}

	@Override
	public boolean filterStatement(SchemaContext pc) {
		PETable tbl = getTarget();
		if (tbl != null) { 
			if (pc.getConnection().isFilteredTable(
					new QualifiedName(
							tbl.getDatabase(pc).getName().getUnquotedName().getUnqualified(),
							tbl.getName().getUnquotedName().getUnqualified()))) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
		final Set<ConvertToAction> complexPlanningActions = getAllActionsOfType(ConvertToAction.class);

		if (complexPlanningActions.isEmpty()) {
			super.plan(sc, es);
		}

		for (final ConvertToAction convertAction : complexPlanningActions) {
			if (!convertAction.isTransientOnly()) {
				final PETable targetTable = this.getTarget();
				final ComplexAlterTableActionCallback actionCallback = new ComplexAlterTableActionCallback(targetTable, convertAction,
						convertAction.getColumnMetadataContainer());
				es.append(new ComplexDDLExecutionStep(targetTable.getPEDatabase(sc), targetTable.getStorageGroup(sc), targetTable, Action.ALTER, actionCallback));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T extends AlterTableAction> Set<T> getAllActionsOfType(final Class<T> clazz) {
		if (actions != null) {
			final Set<T> actionsOfClass = new HashSet<T>();
			for (final AlterTableAction action : actions) {
				if (clazz.isAssignableFrom(action.getClass())) {
					actionsOfClass.add((T) action);
				}
			}

			return actionsOfClass;
		}

		return Collections.emptySet();
	}

	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		SQLCommand sql = getSQLCommand(pc);
		List<CatalogEntity> newObjs = getCatalogObjects(pc);
		List<CatalogEntity> delObjs = getDeleteObjects(pc);
		if (finalActions != null)
			sql = getSQLCommand(pc);
		return new SimpleDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), getRoot(), getAction(), sql,
				delObjs, newObjs ,getInvalidationRecord(pc));
	}

	private static class ComplexAlterTableActionCallback implements NestedOperationDDLCallback {

		private static class ResultMetadataFilter implements OperationFilter {

			private final Map<String, ColumnMetadata> columnMetadata;

			protected ResultMetadataFilter(final Map<String, ColumnMetadata> metadataContainer) {
				if (metadataContainer == null) {
					throw new NullPointerException("Must specify a container for the metadata.");
				}
				this.columnMetadata = metadataContainer;
			}

			@Override
			public void filter(SSConnection ssCon, ColumnSet columnSet, List<ArrayList<String>> rowData, DBResultConsumer results) throws Throwable {
				if (results instanceof MysqlTextResultCollector) {
					final MysqlTextResultCollector metadataCollector = (MysqlTextResultCollector) results;
					for (final ColumnMetadata column : metadataCollector.getColumnSet().getColumnList()) {
						this.columnMetadata.put(column.getName(), column);
					}

					return;
				}
				
				throw new PECodingException("A result collector of type 'MysqlTextResultCollector' is required.");
			}

			@Override
			public String describe() {
				return "ResultMetadataFilter";
			}
		}

		private final PETable alterTarget;
		private final ClonableAlterTableAction alterAction;
		private final List<QueryStep> plan = new ArrayList<QueryStep>();
		private final Map<String, ColumnMetadata> metadata;

		private PEAlterTableStatement alterTargetTableStatement;

		protected ComplexAlterTableActionCallback(final PETable target, final ClonableAlterTableAction action, final Map<String, ColumnMetadata> metadataContainer) {
			this.alterTarget = target;
			this.alterAction = action;
			this.metadata = metadataContainer;
		}

		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg) throws PEException {
			final SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();

			final PETable sampleTarget = this.alterTarget.recreate(sc, this.alterTarget.getDeclaration(), new LockInfo(
					com.tesora.dve.lockmanager.LockType.RSHARED, "transient alter table statement"));
			sampleTarget.setName(new UnqualifiedName(UserTable.getNewTempTableName()));
			sampleTarget.setDeclaration(sc, sampleTarget);

			final TableInstance targetTableInstance = new TableInstance(sampleTarget, sampleTarget.getName(), null, true);

			final PECreateTableStatement createSampleTable = new PECreateTableStatement(sampleTarget, false);

			final PEAlterTableStatement alterSampleTable =
					new PEAlterTableStatement(sc, targetTableInstance.getTableKey(), Collections.singletonList(this.alterAction.makeTransientOnlyClone()));
			
			final SelectStatement selectSampleTableMetadata = new SelectStatement(Collections.singletonList(new FromTableReference(targetTableInstance)),
					Collections.<ExpressionNode> singletonList(new Wildcard(null)), null, null, null, null, null, null, null, null, new AliasInformation(),
					null);

			final PEDropTableStatement dropSampleTable = new PEDropTableStatement(sc, Collections.singletonList(targetTableInstance.getTableKey()), Collections.<Name> emptyList(), true);

			final ExecutionSequence es = new ExecutionSequence(null);

			final PEStorageGroup sg = buildOneSiteGroup(sc, false);

			es.append(new SessionExecutionStep(null, sg, createSampleTable.getSQL(sc)));
			es.append(new SessionExecutionStep(null, sg, alterSampleTable.getSQL(sc)));
			es.append(new FilterExecutionStep(new SessionExecutionStep(null, sg, selectSampleTableMetadata.getSQL(sc)),
					new ResultMetadataFilter(this.metadata)));
			es.append(new SessionExecutionStep(null, sg, dropSampleTable.getSQL(sc)));

			es.schedule(null, this.plan, null, sc);
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			final SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();

			final TableInstance targetTableInstance = new TableInstance(this.alterTarget, this.alterTarget.getName(), null, true);
			this.alterTargetTableStatement = new PEAlterTableStatement(sc, targetTableInstance.getTableKey(),
					Collections.<AlterTableAction> singletonList(this.alterAction));

			final ExecutionSequence es = new ExecutionSequence(null);
			es.append(new SessionExecutionStep(this.alterTarget.getDatabase(sc), this.alterTarget.getStorageGroup(sc), this.alterTargetTableStatement
					.getSQL(sc)));

			es.schedule(null, this.plan, null, sc);
		}

		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return Collections.emptyList();
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			return Collections.emptyList();
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return SQLCommand.EMPTY;
		}

		@Override
		public boolean canRetry(Throwable t) {
			return true;
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		@Override
		public String description() {
			return "Perform an ALTER operation on a native table and collect the column metadata.";
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return new CacheInvalidationRecord(this.alterTarget.getCacheKey(), InvalidationScope.LOCAL);
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

		@Override
		public void postCommitAction(CatalogDAO c) throws PEException {
			// TODO Auto-generated method stub
		}

		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
			for (final QueryStep step : this.plan) {
				final QueryStepOperation qso = step.getOperation();
				qso.execute(conn, wg, new MysqlTextResultCollector());
			}

			// The metadata should have already been loaded, so update the
			// catalog records.
			final SchemaContext sc = SchemaContext.createContext(conn);
			this.alterTargetTableStatement.getCatalogEntries(sc);
			this.alterTargetTableStatement.getDeleteObjects(sc);
		}
	}
}
