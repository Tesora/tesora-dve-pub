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

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepMultiInsertByKeyOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.JustInTimeInsert;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;

public class LateSortingInsertExecutionStep extends DirectExecutionStep {

	// if true we ignore whatever update count we get after sorting
	protected boolean ignoreUpdateCount;
	
	private LateSortingInsertExecutionStep(SchemaContext pc, Database<?> db, PEStorageGroup tsg, PETable tab, boolean ignoreUpdateCount) throws PEException {
		super(db, tsg, ExecutionType.INSERT, tab.getDistributionVector(pc), null, null, null);
		this.ignoreUpdateCount = ignoreUpdateCount;
	}

	public static LateSortingInsertExecutionStep build(SchemaContext pc, Database<?> db, PEStorageGroup tsg, PETable tab,
			boolean ignoreUpdateCount) throws PEException {
		return new LateSortingInsertExecutionStep(pc, db, tsg, tab, ignoreUpdateCount);
	}
	
	@Override
	public Long getlastInsertId(ValueManager vm, SchemaContext sc) {
		return vm.getLastInsertId(sc);
	}
	
	private List<JustInTimeInsert> getLateInserts(SchemaContext sc) {
		return sc.getValueManager().getLateSortedInsert(sc);
	}
	
	@Override
	public Long getUpdateCount(SchemaContext sc) {
		if (ignoreUpdateCount)
			return null;
		List<JustInTimeInsert> late = getLateInserts(sc);
		long uc = 0;
		for(JustInTimeInsert jti : late)
			uc += jti.getUpdateCount();
		return uc;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		List<JustInTimeInsert> late = getLateInserts(sc);

		QueryStepMultiInsertByKeyOperation qso = new QueryStepMultiInsertByKeyOperation(getStorageGroup(sc),getPersistentDatabase());
		for(JustInTimeInsert jti : late) {
			SQLCommand sqlc = jti.getSQL();
			sqlc.withReferenceTime(sc.getValueManager().getCurrentTimestamp(sc));
			qso.addStatement(jti.getKey().getDetachedKey(sc), sqlc);
		}
		qso.setStatistics(getStepStatistics(sc));
		qsteps.add(qso);
	}
	
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		String execType = getEffectiveExecutionType().name();
		StringBuilder prefix = new StringBuilder();
		prefix.append(indent).append(execType).append(" on ").append((getDatabase() == null ? "null" : getDatabase().getName().get()))
			.append("/").append(getStorageGroup(sc));
		for(JustInTimeInsert jti : getLateInserts(sc)) {
			buf.add(prefix.toString());
			if (opts == null)
				buf.add(indent + "  sql: '" + jti.getSQL() + "'");
			else {
				buf.add(indent + "  sql:");
				buf.add(jti.getSQL().getRawSQL());
			}
			buf.add(indent + " dist key: " + jti.getKey().describe(sc.getValues()));
		}
	}
}
