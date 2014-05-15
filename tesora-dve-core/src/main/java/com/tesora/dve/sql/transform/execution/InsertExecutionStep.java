// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;



import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepInsertByKeyOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public final class InsertExecutionStep extends DirectExecutionStep {

	// number of tuples
	protected Long updateCount;

	private InsertExecutionStep(SchemaContext pc, Database<?> db, PEStorageGroup tsg, DMLStatement sql, PETable tab, DistributionKey kv, 
			Long uc) throws PEException {
		super(db, tsg, ExecutionType.INSERT, tab.getDistributionVector(pc), kv,
				sql.getGenericSQL(pc, false, true), null);
		updateCount = uc;
	}

	public static InsertExecutionStep build(SchemaContext pc, Database<?> db, PEStorageGroup tsg,
			DMLStatement sql, PETable tab, DistributionKey kv, Long uc) throws PEException {
		// tenant rewrite not needed due to special insert planning
		return new InsertExecutionStep(pc, db, tsg, sql, tab, kv, uc);
	}
	
	@Override
	public Long getlastInsertId(ValueManager vm, SchemaContext sc) {
		return vm.getLastInsertId(sc);
	}
	
	@Override
	public Long getUpdateCount(SchemaContext sc) {
		return updateCount;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		SQLCommand sqlCommand = getCommand(sc).withReferenceTime(getReferenceTimestamp(sc));
		QueryStepInsertByKeyOperation qso = 
				new QueryStepInsertByKeyOperation(getPersistentDatabase(), 
						getKeyValue(sc), 
						sqlCommand);
		qso.setStatistics(getStepStatistics(sc));
		addStep(sc,qsteps, qso);		
	}
}
