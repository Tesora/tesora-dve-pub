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
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDMLOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
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
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		QueryStepDMLOperation qso = null;
		IKeyValue ikv = getKeyValue(sc);
		SQLCommand sqlCommand = getCommand(sc).withReferenceTime(getReferenceTimestamp(sc));
		StorageGroup sg = getStorageGroup(sc);
		if (ikv != null)
			qso = new QueryStepUpdateByKeyOperation(sg, getPersistentDatabase(), ikv, sqlCommand);
		else {
			sc.beginSaveContext();
			try {
				qso = new QueryStepUpdateAllOperation(sg, getPersistentDatabase(), 
						table.getDistributionVector(sc).getModel().getSingleton(), sqlCommand);
			} finally {
				sc.endSaveContext();
			}
		}
		qso.setStatistics(getStepStatistics(sc));
		qsteps.add(qso);
	}

	@Override
	public void prepareForCache() {
		if (table != null)
			table.setFrozen();
		super.prepareForCache();
	}
	
}
