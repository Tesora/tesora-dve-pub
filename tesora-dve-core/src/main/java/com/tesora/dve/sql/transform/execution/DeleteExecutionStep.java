// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;


import java.util.List;

import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepUpdateAllOperation;
import com.tesora.dve.queryplan.QueryStepUpdateByKeyOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public final class DeleteExecutionStep extends DirectExecutionStep {

	private PETable table;
	
	public static DeleteExecutionStep build(SchemaContext sc, Database<?> db, PEStorageGroup storageGroup,
			TableKey tk, DistributionKey distKey, DMLStatement command,
			boolean requiresReferenceTimestamp, DMLExplainRecord splain) throws PEException {
		maybeApplyMultitenant(sc,command);
		return new DeleteExecutionStep(sc, db, storageGroup, tk, distKey, command.getGenericSQL(sc, false, true), requiresReferenceTimestamp, splain);
	}
	
	private DeleteExecutionStep(SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, TableKey tk, DistributionKey distKey,
			GenericSQLCommand command, boolean requiresReferenceTimestamp,
			DMLExplainRecord splain) throws PEException {
		super(db, storageGroup, ExecutionType.DELETE, tk.getAbstractTable().getDistributionVector(sc), distKey, command, requiresReferenceTimestamp, splain);
		table = tk.getAbstractTable().asTable();
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		QueryStepOperation qso = null;
		IKeyValue kv = getKeyValue(sc);
		SQLCommand sqlCommand = getCommand(sc).withReferenceTime(getReferenceTimestamp(sc));
		if (kv != null)
			qso = new QueryStepUpdateByKeyOperation(getPersistentDatabase(), kv, sqlCommand);
		else {
			qso = new QueryStepUpdateAllOperation(getPersistentDatabase(),
					table.getDistributionVector(sc).getPersistent(sc), sqlCommand);
		}
		addStep(sc,qsteps, qso);

	}
	
	@Override
	public Long getUpdateCount(SchemaContext sc) {
		return super.getUpdateCount(sc);
	}

	@Override
	public void prepareForCache() {
		table.setFrozen();
		super.prepareForCache();
	}
	
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, buf, indent, opts);
	}

}
