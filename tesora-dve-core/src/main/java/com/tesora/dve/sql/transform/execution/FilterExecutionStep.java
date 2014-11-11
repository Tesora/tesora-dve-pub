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
import java.util.Set;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepFilterOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepFilterOperation.OperationFilter;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public class FilterExecutionStep extends ExecutionStep {

	private ExecutionStep source;
	private OperationFilter filter;
	
	public FilterExecutionStep(ExecutionStep src, OperationFilter filter) {
		super(src.getDatabase(), src.getPEStorageGroup(), src.getExecutionType());
		source = src;
		this.filter = filter;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cvm, ExecutionPlan containing) throws PEException {
		ArrayList<QueryStepOperation> sub = new ArrayList<QueryStepOperation>();
		source.schedule(opts, sub,projection,sc, cvm, containing);
		for(int i = 0; i < sub.size() - 1; i++) {
			qsteps.add(sub.get(i));
		}
		QueryStepOperation qso = sub.get(sub.size() - 1);
		OperationFilter actual = filter;
		if (filter instanceof LateBindingOperationFilter) {
			LateBindingOperationFilter lbof = (LateBindingOperationFilter) filter;
			actual = lbof.adapt(opts, qso);
		}
		qsteps.add(new QueryStepFilterOperation(qso,actual));
	}

	@Override
	public void display(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, String indent, EmitOptions opts) {
		source.display(sc, cvm, containing, buf, indent, opts);
	}
	
	@Override
	public void explain(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<ResultRow> rows, ExplainOptions opts) {
		source.explain(sc,cvm,containing,rows, opts);
	}
		
	public OperationFilter getFilter() {
		return filter;
	}
	
	public ExecutionStep getSource() {
		return source;
	}
	
	public abstract static class LateBindingOperationFilter implements OperationFilter {
		
		public static void schedule(PlannerContext pc, FeatureStep child, ExecutionSequence es, Set<FeatureStep> scheduled, LateBindingOperationFilter filter) throws PEException {
			final ExecutionSequence temp = new ExecutionSequence(null);
			child.schedule(pc, temp, scheduled);
			final DirectExecutionStep lastStep = (DirectExecutionStep) temp.getLastStep();
			for (final HasPlanning step : temp.getSteps()) {
				if (step != lastStep) {
					es.append(step);
				}
			}
			es.append(new FilterExecutionStep(lastStep, filter));
		}
		
		public abstract OperationFilter adapt(ExecutionPlanOptions opts, QueryStepOperation bindTo) throws PEException;

		@Override
		public void filter(SSConnection ssCon, ColumnSet columnSet, List<ArrayList<String>> rowData, DBResultConsumer results)
				throws Throwable {
			throw new IllegalStateException("LateBindingOperationFilter doesn't support direct filtering");
		}

	}
}
