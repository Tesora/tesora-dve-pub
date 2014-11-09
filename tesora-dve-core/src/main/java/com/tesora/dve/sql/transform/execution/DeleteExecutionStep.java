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

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepUpdateAllOperation;
import com.tesora.dve.queryplan.QueryStepUpdateByKeyOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.ConnectionValues;
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
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValues cv)
			throws PEException {
		QueryStepOperation qso = null;
		IKeyValue kv = getKeyValue(sc,cv);
		SQLCommand sqlCommand = getCommand(sc,cv).withReferenceTime(getReferenceTimestamp(cv));
		StorageGroup sg = getStorageGroup(sc,cv);
		if (kv != null)
			qso = new QueryStepUpdateByKeyOperation(sg,getPersistentDatabase(), kv, sqlCommand);
		else {
			qso = new QueryStepUpdateAllOperation(sg, getPersistentDatabase(),
					table.getDistributionVector(sc).getPersistent(sc), sqlCommand);
		}
		qsteps.add(qso);
	}
	
	@Override
	public Long getUpdateCount(SchemaContext sc,ConnectionValues cv) {
		return super.getUpdateCount(sc,cv);
	}

	@Override
	public void prepareForCache() {
		table.setFrozen();
		super.prepareForCache();
	}
	
	@Override
	public void display(SchemaContext sc, ConnectionValues cv, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, cv, buf, indent, opts);
	}

}
