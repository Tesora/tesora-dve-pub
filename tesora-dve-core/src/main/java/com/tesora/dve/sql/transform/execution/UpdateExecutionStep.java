// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;


import java.util.List;

import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDMLOperation;
import com.tesora.dve.queryplan.QueryStepUpdateAllOperation;
import com.tesora.dve.queryplan.QueryStepUpdateByKeyOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public final class UpdateExecutionStep extends DirectExecutionStep {

	private PETable table;

	private UpdateExecutionStep(SchemaContext pc, 
			Database<?> db, 
			PEStorageGroup storageGroup, 
			PETable tab, 
			DistributionKey kv, 
			DMLStatement command, 
			boolean requiresReferenceTimestamp,
			DMLExplainRecord splain) throws PEException {
		super(db, storageGroup, ExecutionType.UPDATE, tab.getDistributionVector(pc), kv, command.getGenericSQL(pc, false, true), requiresReferenceTimestamp, splain);
		table = tab;
	}

	public static UpdateExecutionStep build(SchemaContext pc, Database<?> db, PEStorageGroup storageGroup,
			PETable tab, DistributionKey kv, DMLStatement command, boolean requiresReferenceTimestamp,
			DMLExplainRecord splain) throws PEException {
		maybeApplyMultitenant(pc,command);
		return new UpdateExecutionStep(pc, db, storageGroup, tab, kv, command, requiresReferenceTimestamp, splain);
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		QueryStepDMLOperation qso = null;
		IKeyValue ikv = getKeyValue(sc);
		SQLCommand sqlCommand = getCommand(sc).withReferenceTime(getReferenceTimestamp(sc));
		if (ikv != null)
			qso = new QueryStepUpdateByKeyOperation(getPersistentDatabase(), ikv, sqlCommand);
		else {
			sc.beginSaveContext();
			try {
				qso = new QueryStepUpdateAllOperation(getPersistentDatabase(), 
						table.getDistributionVector(sc).getModel().getSingleton(), sqlCommand);
			} finally {
				sc.endSaveContext();
			}
		}
		qso.setStatistics(getStepStatistics(sc));
		addStep(sc,qsteps, qso);
	}

	@Override
	public void prepareForCache() {
		if (table != null)
			table.setFrozen();
		super.prepareForCache();
	}
	
}
