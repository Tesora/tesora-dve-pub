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

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.ConnectionValues;
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

	protected SQLCommand getSQLCommand(final SchemaContext sc) {
		return new SQLCommand(sc, sql);
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cvm, ExecutionPlan containing)
			throws PEException {
		ConnectionValues cv = cvm.getValues(containing);
		qsteps.add(new QueryStepSelectAllOperation(getStorageGroup(sc,cv),getPersistentDatabase(), distributionModel, getSQLCommand(sc)));
	}

	@Override
	public void getSQL(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, EmitOptions opts) {
		buf.add(sql);
	}

	public SessionExecutionStep onSingleSite() {
		distributionModel = BroadcastDistributionModel.SINGLETON;
		return this;
	}

}
