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

import java.util.ArrayList;
import java.util.List;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepTriggerOperation;
import com.tesora.dve.queryplan.TriggerValueHandlers;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.UnaryProcedure;

public class TriggerExecutionStep extends ExecutionStep {

	// the select I will execute to get the rows
	private ExecutionStep rowQuery;
	private TriggerValueHandlers handlers;
	// any before step
	private HasPlanning before;
	// the actual step
	private ExecutionStep actual;
	// any after step
	private HasPlanning after;
	
	public TriggerExecutionStep(Database<?> db, PEStorageGroup storageGroup, 
			ExecutionStep actual,
			HasPlanning before,
			HasPlanning after,
			ExecutionStep rowQuery,
			TriggerValueHandlers handlers) {
		super(db, storageGroup, ExecutionType.TRIGGER);
		this.actual = actual;
		this.before = before;
		this.after = after;
		this.rowQuery = rowQuery;
		this.handlers = handlers;
	}

	// accessors used in the tests
	public ExecutionStep getRowQuery() {
		return rowQuery;
	}
	
	public ExecutionStep getActualStep() {
		return actual;
	}
	
	public HasPlanning getBeforeStep() {
		return before;
	}
	
	public HasPlanning getAfterStep() {
		return after;
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps,
			ProjectionInfo projection, SchemaContext sc,ConnectionValuesMap cvm, ExecutionPlan containing) throws PEException {
		QueryStepTriggerOperation trigOp = new QueryStepTriggerOperation(handlers,
				buildOperation(opts,sc,cvm,containing,rowQuery),
				(before == null ? null : buildOperation(opts,sc,cvm,containing,before)),
				buildOperation(opts,sc,cvm,containing,actual),
				(after == null ? null : buildOperation(opts,sc,cvm,containing,after)));
		qsteps.add(trigOp);
	}

	private QueryStepOperation buildOperation(ExecutionPlanOptions opts, SchemaContext sc, 
			ConnectionValuesMap cvm, ExecutionPlan containing, HasPlanning toSchedule) throws PEException {
		List<QueryStepOperation> sub = new ArrayList<QueryStepOperation>();
		toSchedule.schedule(opts,sub,null,sc,cvm,containing);
		return 	RootExecutionPlan.collapseOperationList(sub);
	}
	
	public void display(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, cvm,containing,buf, indent, opts);
		String sub1 = indent + "  ";
		String sub2 = sub1 + "  ";
		buf.add(sub1 + "Row query");
		rowQuery.display(sc,cvm,containing,buf,sub2,opts);
		if (before != null) {
			buf.add(sub1 + "Before");
			before.display(sc,cvm,containing,buf,sub2,opts);
		}
		buf.add(sub1 + "Actual");
		actual.display(sc,cvm,containing,buf,sub2,opts);
		if (after != null) {
			buf.add(sub1 + "After");
			after.display(sc,cvm,containing,buf,sub2,opts);
		}
	}
	
	public void displaySQL(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
	}
	
	public void explain(SchemaContext sc, List<ResultRow> rows, ExplainOptions opts) {
		
	}

	public void prepareForCache() {
		if (before != null)
			before.prepareForCache();
		actual.prepareForCache();
		if (after != null)
			actual.prepareForCache();
	}
	
	public void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc) {
		if (before != null)
			before.visitInExecutionOrder(proc);
		actual.visitInExecutionOrder(proc);
		if (after != null)
			after.visitInExecutionOrder(proc);
	}

}
