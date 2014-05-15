// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepFilterOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepFilterOperation.OperationFilter;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnection;
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
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc) throws PEException {
		ArrayList<QueryStep> sub = new ArrayList<QueryStep>();
		source.schedule(opts, sub,projection,sc);
		for(int i = 0; i < sub.size() - 1; i++) {
			addStep(sc, qsteps, sub.get(i).getOperation());
		}
		QueryStep last = sub.get(sub.size() - 1);
		QueryStepOperation qso = last.getOperation();
		OperationFilter actual = filter;
		if (filter instanceof LateBindingOperationFilter) {
			LateBindingOperationFilter lbof = (LateBindingOperationFilter) filter;
			actual = lbof.adapt(opts, qso);
		}
		addStep(sc, qsteps, new QueryStepFilterOperation(qso,actual));
	}

	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		source.display(sc, buf, indent, opts);
	}
	
	@Override
	public void explain(SchemaContext sc, List<ResultRow> rows, ExplainOptions opts) {
		source.explain(sc,rows, opts);
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
