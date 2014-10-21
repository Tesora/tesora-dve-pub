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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.UnaryProcedure;

public class TriggerExecutionStep extends ExecutionStep {

	// the temp table step
	private RedistributionExecutionStep tempTable;
	// column key to offset within the temp table mapping
	private Map<ColumnKey,Integer> columnOffsets;
	// any before step
	private ExecutionStep before;
	// the actual step
	private ExecutionStep actual;
	// any after step
	private ExecutionStep after;
	
	public TriggerExecutionStep(Database<?> db, PEStorageGroup storageGroup, 
			RedistributionExecutionStep srcTab,
			ExecutionStep actual,
			ExecutionStep before,
			ExecutionStep after,
			Map<ColumnKey,Integer> offsets) {
		super(db, storageGroup, ExecutionType.TRIGGER);
		this.tempTable = srcTab;
		this.actual = actual;
		this.before = before;
		this.after = after;
		this.columnOffsets = offsets;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps,
			ProjectionInfo projection, SchemaContext sc) throws PEException {
		// the other stuff then has to be turned into qsos and tossed into the trigger qso
		throw new PEException("Not quite yet");
	}

	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, buf, indent, opts);
		String sub = indent + "  ";
		buf.add(indent + "Temp Table");
		tempTable.display(sc, buf, sub, opts);
		if (before != null) {
			buf.add(indent + "Before");
			before.display(sc,buf,sub,opts);
		}
		buf.add(indent + "Actual");
		actual.display(sc,buf,sub,opts);
		if (after != null) {
			buf.add(indent + "After");
			after.display(sc,buf,sub,opts);
		}
	}
	
	public void explain(SchemaContext sc, List<ResultRow> rows, ExplainOptions opts) {
		
	}

	public void prepareForCache() {
		tempTable.prepareForCache();
		if (before != null)
			tempTable.prepareForCache();
		actual.prepareForCache();
		if (after != null)
			actual.prepareForCache();
	}
	
	public void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc) {
		tempTable.visitInExecutionOrder(proc);
		if (before != null)
			before.visitInExecutionOrder(proc);
		actual.visitInExecutionOrder(proc);
		if (after != null)
			after.visitInExecutionOrder(proc);
	}
	

}
