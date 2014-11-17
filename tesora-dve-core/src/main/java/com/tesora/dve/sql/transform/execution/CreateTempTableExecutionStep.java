package com.tesora.dve.sql.transform.execution;

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

import java.util.List;

import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepCreateTempTableOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.ConnectionValues;
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
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cvm, ExecutionPlan containing) throws PEException {
		// we need to persist out the table each time rather than caching it due to the generated names.  avoid leaking
		// persistent entities by using a new ddl context.
		SchemaContext tsc = SchemaContext.makeMutableIndependentContext(sc);
		ConnectionValues cv = cvm.getValues(containing);
		// set the values so that we get the updated table name
		tsc.setValues(cv);
		UserTable ut = theTable.getPersistent(tsc);
		QueryStepOperation qso = new QueryStepCreateTempTableOperation(getStorageGroup(sc,cv),ut);
		qsteps.add(qso);
	}

	@Override
	public void getSQL(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, EmitOptions opts) {
		buf.add("EXPLICIT TEMP TABLE: " +	Singletons.require(HostService.class).getDBNative().getEmitter().emitCreateTableStatement(sc, sc.getValues(), theTable));
	}

	
}
