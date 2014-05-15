// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.List;

import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepCreateTempTableOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;

public class CreateTempTableExecutionStep extends ExecutionStep {

	private PETable theTable;
	
	public CreateTempTableExecutionStep(Database<?> db, PEStorageGroup storageGroup, PETable toCreate) {
		super(db, storageGroup, ExecutionType.DDL);
		theTable = toCreate;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		// we need to persist out the table each time rather than caching it due to the generated names.  avoid leaking
		// persistent entities by using a new ddl context.
		SchemaContext tsc = SchemaContext.makeMutableIndependentContext(sc);
		// set the values so that we get the updated table name
		tsc.setValues(sc._getValues());
		UserTable ut = theTable.getPersistent(tsc);
		QueryStepOperation qso = new QueryStepCreateTempTableOperation(ut);
		addStep(sc,qsteps, qso);
	}

	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add("EXPLICIT TEMP TABLE: " +	Singletons.require(HostService.class).getDBNative().getEmitter().emitCreateTableStatement(sc, theTable));
	}

	
}
