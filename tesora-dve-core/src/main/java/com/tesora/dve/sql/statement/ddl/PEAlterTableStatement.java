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

import org.apache.commons.collections.ListUtils;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.queryplan.QueryStepFilterOperation.OperationFilter;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TableComponent;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction.ClonableAlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.ConvertToAction;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
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
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		final Set<ConvertToAction> complexPlanningActions = getAllActionsOfType(ConvertToAction.class);

		if (complexPlanningActions.isEmpty()) {
			super.plan(sc, es, config);
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

	private static class ComplexAlterTableActionCallback extends NestedOperationDDLCallback {

		private static class ColumnTypeMetadataCollector implements OperationFilter {

			private final PETable forTable;
			private final Map<String, Type> columnMetadata;

			protected ColumnTypeMetadataCollector(final PETable forTable, final Map<String, Type> metadataContainer) {
				if (forTable == null) {
					throw new PECodingException("Must provide the source table.");
				} else if (metadataContainer == null) {
					throw new PECodingException("Must provide a container for the metadata.");
				}

				this.forTable = forTable;
				this.columnMetadata = metadataContainer;
			}

			public FilterExecutionStep getCollectorExecutionStep(final SchemaContext sc) throws PEException {
				return new FilterExecutionStep(ProjectingExecutionStep.build(sc, null, this.forTable.getStorageGroup(sc), this.buildSQL()), this);
			}

			private String buildSQL() {
				return "SHOW COLUMNS IN " + this.forTable.getName().getSQL() + "";
			}

			@Override
			public void filter(SSConnection ssCon, ColumnSet columnSet, List<ArrayList<String>> rowData, DBResultConsumer results) throws Throwable {
				for (final List<String> row : rowData) {
					final String columnName = row.get(0);
					final ParserOptions options = ParserOptions.NONE.setDebugLog(true).setResolve().setFailEarly().setActualLiterals();
					final Type columnType = InvokeParser.parseType(null, options, row.get(1));
					this.columnMetadata.put(columnName, columnType);
				}
			}

			@Override
			public String describe() {
				return "ColumnTypeMetadataCollector";
			}
		}

		private final PETable alterTarget;
		private final ClonableAlterTableAction alterAction;
		private final List<QueryStep> plan = new ArrayList<QueryStep>();
		private final Map<String, Type> metadata;

		private PEAlterTableStatement alterTargetTableStatement;

		protected ComplexAlterTableActionCallback(final PETable target, final ClonableAlterTableAction action,
				final Map<String, Type> metadataContainer) {
			this.alterTarget = target;
			this.alterAction = action;
			this.metadata = metadataContainer;
		}

		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg) throws PEException {
			final SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();

			/*
			 * Build a sample table on which we then perform a trial conversion
			 * to get the resultant column types.
			 */
			final PETable sampleTarget = this.alterTarget.recreate(sc, this.alterTarget.getDeclaration(), new LockInfo(
					com.tesora.dve.lockmanager.LockType.RSHARED, "transient alter table statement"));

			final PEDatabase sampleTargetDatabase = sampleTarget.getPEDatabase(sc);
			final List<TableModifier> sampleTargetModifiers = new ArrayList<TableModifier>();
			for (final TableModifier entry : sampleTarget.getModifiers().getModifiers()) {
				if (entry != null) {
					sampleTargetModifiers.add(entry);
				}
			}

			/* FKs would not resolve on the sample site. */
			sampleTarget.removeForeignKeys(sc);

			final List<TableComponent<?>> sampleTargetFieldsAndKeys = ListUtils.union(sampleTarget.getColumns(sc), sampleTarget.getKeys(sc));
			final QualifiedName sampleTargetName = new QualifiedName(
					sampleTargetDatabase.getName().getUnqualified(),
					new UnqualifiedName(UserTable.getNewTempTableName())
					);

			final PEPersistentGroup sg = buildOneSiteGroup(sc, false);

			/*
			 * Make sure the actual sample gets created as a TEMPORARY table in
			 * the user database so that it always gets removed.
			 */
			final ComplexPETable temporarySampleTarget = new ComplexPETable(sc,
					sampleTargetName, sampleTargetFieldsAndKeys,
					sampleTarget.getDistributionVector(sc), sampleTargetModifiers,
					sg, sampleTargetDatabase, TableState.SHARED);
			temporarySampleTarget.withTemporaryTable(sc);

			final TableInstance targetTableInstance = new TableInstance(temporarySampleTarget, temporarySampleTarget.getName(), null, true);

			final PECreateTableStatement createSampleTable = new PECreateTableStatement(temporarySampleTarget, false);

			final PEAlterTableStatement alterSampleTable =
					new PEAlterTableStatement(sc, targetTableInstance.getTableKey(), Collections.singletonList(this.alterAction.makeTransientOnlyClone()));

			final ColumnTypeMetadataCollector metadataFilter = new ColumnTypeMetadataCollector(temporarySampleTarget, this.metadata);

			final PEDropTableStatement dropSampleTable = new PEDropTableStatement(sc, Collections.singletonList(targetTableInstance.getTableKey()), Collections.<Name> emptyList(), true, 
					targetTableInstance.getTableKey().isUserlandTemporaryTable());

			final ExecutionSequence es = new ExecutionSequence(null);
			
			es.append(new SessionExecutionStep(null, sg, createSampleTable.getSQL(sc)));
			es.append(new SessionExecutionStep(null, sg, alterSampleTable.getSQL(sc)));
			es.append(metadataFilter.getCollectorExecutionStep(sc));
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
		public boolean canRetry(Throwable t) {
			return false;
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
