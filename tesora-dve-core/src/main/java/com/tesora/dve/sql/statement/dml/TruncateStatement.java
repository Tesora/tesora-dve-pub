// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;


import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.statement.StatementTraits;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.LateBindingUpdateCountFilter.LateBindingUpdateCounter;
import com.tesora.dve.sql.transform.strategy.DegenerateExecuteTransformFactory;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.TruncateStatementMTTransformFactory;
import com.tesora.dve.worker.WorkerGroup;

public class TruncateStatement extends UnaryTableDMLStatement {

	public static class ResetAutoIncrementTrackerCallback implements NestedOperationDDLCallback {

		private final PETable table;
		private final TableScope scope;

		public ResetAutoIncrementTrackerCallback(final TableKey tk) {
			this.table = tk.getAbstractTable().asTable();
			this.scope = (tk instanceof MTTableKey) ? ((MTTableKey) tk).getScope() : null;
		}

		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg) throws PEException {
			// TODO Auto-generated method stub

		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			// TODO Auto-generated method stub
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
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean requiresFreshTxn() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String description() {
			return "Reset the AI tracker of a given table.";
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			final SchemaCacheKey<?> cacheKey = (this.scope != null) ? this.scope.getCacheKey() : this.table.getCacheKey();
			return new CacheInvalidationRecord(cacheKey, InvalidationScope.LOCAL);
		}

		@Override
		public boolean requiresWorkers() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void postCommitAction(CatalogDAO c) throws PEException {
			// TODO Auto-generated method stub

		}

		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
			final SchemaContext sc = SchemaContext.createContext(conn);
			if (table.hasAutoInc()) {
				AutoIncrementTracker ait = null;
				TableScope actualScope = null;
				if (scope != null) {
					final PETenant currentTenant = (PETenant) sc.getCurrentTenant().get(sc);
					actualScope = currentTenant.lookupScope(sc, scope.getName(), new LockInfo(StatementTraits.getLockType(TruncateStatement.class), "truncate statement callback"));
				}
				if (actualScope != null)
					ait = sc.getCatalog().getBackingTracker(sc, actualScope);
				else
					ait = sc.getCatalog().getBackingTracker(sc, table);
				if (ait != null) {
					ait.reset(sc.getCatalog().getDAO());
				}
			}
		}

	}

	public TruncateStatement(TableInstance tab, SourceLocation loc) {
		super(loc);
		this.intoTable.set(tab);
	}
	
	public TableInstance getTruncatedTable() {
		return intoTable.get();
	}

	@Override
	public List<PEStorageGroup> getStorageGroups(SchemaContext pc) {
		return Collections.singletonList(getTable().getStorageGroup(pc));
	}

	
	@Override
	public void normalize(SchemaContext pc) {
		// no-op
	}
	
	/**
	 * Although TRUNCATE TABLE is similar to DELETE, it is classified as a DDL
	 * statement rather than a DML statement.
	 */
	@Override
	public boolean isDML() {
		return false;
	}

	/**
	 * Any AUTO_INCREMENT value is reset to its start value. This is true even
	 * for MyISAM and InnoDB, which normally do not reuse sequence values.
	 */
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		planViaTransforms(pc, this, es);

		/*
		 * TRUNCATE always returns 0 update count.
		 * Make sure, we use the count from this step
		 * even in the MT case.
		 */
		final TableInstance truncatedTable = getTruncatedTable();
		if (!truncatedTable.isMT()) {
			es.append(new SimpleDDLExecutionStep((PEDatabase) getDatabase(pc), getStorageGroup(pc), truncatedTable.getAbstractTable(), Action.ALTER,
					getSQLCommand(pc),
					Collections.<CatalogEntity> emptyList(), Collections.<CatalogEntity> emptyList(), 
					new CacheInvalidationRecord(truncatedTable
							.getAbstractTable().getCacheKey(), InvalidationScope.LOCAL)));
		}
		es.append(new FilterExecutionStep(buildResetAutoIncrementTrackerStep(pc), new LateBindingUpdateCounter()));
	}

	@Override
	public ExecutionStep buildSingleKeyStep(SchemaContext sc, TableKey tab, DistributionKey kv, DMLStatement sql) throws PEException {
		return buildResetAutoIncrementTrackerStep(sc);
	}

	@Override
	public ExecutionType getExecutionType() {
		return ExecutionType.DELETE;
	}

	@Override
	public TransformFactory[] getTransformers() {
		return new TransformFactory[] {
				new TruncateStatementMTTransformFactory(),
				new DegenerateExecuteTransformFactory()
		};
	}

	@Override
	public DistKeyOpType getKeyOpType() {
		return null;
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.TRUNCATE;
	}

	public ExecutionStep buildResetAutoIncrementTrackerStep(final SchemaContext sc) throws PEException {
		return new ComplexDDLExecutionStep((PEDatabase) getDatabase(sc), getStorageGroup(sc), getTruncatedTable().getAbstractTable(), Action.ALTER,
				new ResetAutoIncrementTrackerCallback(getTruncatedTable().getTableKey()));
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return true;
	}

	@Override
	protected int selfHashCode() {
		return 0;
	}

}
