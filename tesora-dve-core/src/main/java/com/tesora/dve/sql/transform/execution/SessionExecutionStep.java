// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;


import java.util.List;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;

public class SessionExecutionStep extends ExecutionStep {

	private String sql;
	
	private DistributionModel distributionModel = StaticDistributionModel.SINGLETON;
	
	public SessionExecutionStep(Database<?> db, PEStorageGroup storageGroup, String sql) {
		super(db, storageGroup, ExecutionType.SESSION);
		this.sql = sql;
	}

	protected SQLCommand getSQLCommand() {
		return new SQLCommand(sql);
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		addStep(sc,qsteps,new QueryStepSelectAllOperation(getPersistentDatabase(), distributionModel, getSQLCommand()));
	}

	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add(sql);
	}

	public SessionExecutionStep onSingleSite() {
		distributionModel = BroadcastDistributionModel.SINGLETON;
		return this;
	}

}
