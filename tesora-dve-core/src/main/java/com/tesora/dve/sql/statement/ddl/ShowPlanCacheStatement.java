// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.PlanCacheKey;
import com.tesora.dve.sql.schema.cache.RegularCachedPlan;
import com.tesora.dve.sql.schema.cache.SchemaCache;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.StepExecutionStatistics;
import com.tesora.dve.sql.util.UnaryProcedure;

public class ShowPlanCacheStatement extends SchemaQueryStatement {

	private boolean stats;
	
	public ShowPlanCacheStatement(boolean showStats) {
		super(true, "PLAN CACHE", null);
		this.stats = showStats;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		SchemaCache global = SchemaSourceFactory.peekGlobalCache();
		ArrayList<ResultRow> rows = new ArrayList<ResultRow>();
		Accumulator acc = new Accumulator(pc,stats, rows);
		global.applyOnPlans(acc);
		ColumnSet cs = new ColumnSet();
		cs.addColumn("Database",255,"varchar",Types.VARCHAR);
		cs.addColumn("Query",255,"varchar",Types.VARCHAR);

		ExecutionPlan.addExplainColumnHeaders(cs);
		if (stats)
			StepExecutionStatistics.addColumnHeaders(cs);
		es.append(new DDLQueryExecutionStep("PLAN CACHE",new IntermediateResultSet(cs,rows)));
	}

	private static class Accumulator extends UnaryProcedure<RegularCachedPlan> {
		
		private final boolean statsToo;
		private final List<ResultRow> rows;
		private final ExplainOptions opts;
		private final SchemaContext cntxt;
		
		public Accumulator(SchemaContext sc, boolean doStats, List<ResultRow> acc) {
			this.statsToo = doStats;
			this.rows = acc;
			if (statsToo)
				this.opts = ExplainOptions.NONE.setPlanCache().setStatistics();
			else
				this.opts = ExplainOptions.NONE.setPlanCache();
			cntxt = sc;
		}

		@Override
		public void execute(RegularCachedPlan object) {
			List<ExtractedLiteral.Type> types = object.getLiteralTypes();
			if (types == null) return;
			PlanCacheKey pck = object.getKey();
			String dbName = pck.getDatabase().getName().getUnqualified().getUnquotedName().get();
			SchemaContext sc = SchemaContext.makeImmutableIndependentContext(cntxt);
			ArrayList<ExtractedLiteral> fakes = new ArrayList<ExtractedLiteral>();
			for(int i = 0; i < types.size(); i++) {
				ExtractedLiteral.Type t = types.get(i);
				if (t == ExtractedLiteral.Type.STRING) {
					fakes.add(ExtractedLiteral.makeStringLiteral("'" + Integer.toString(i) + "'", -1));
				} else if (t == ExtractedLiteral.Type.DECIMAL) {
					fakes.add(ExtractedLiteral.makeDecimalLiteral(Double.toString(i * 1.0), -1));
				} else if (t == ExtractedLiteral.Type.INTEGRAL) {
					fakes.add(ExtractedLiteral.makeStringLiteral(Integer.toString(i), -1));
				} else if (t == ExtractedLiteral.Type.HEX) {
					fakes.add(ExtractedLiteral.makeHexLiteral(Integer.toString(i), -1));
				} else {
					throw new SchemaException(Pass.PLANNER, "Unknown extracted literal type: " + t);
				}
			}
			ExecutionPlan ep = null;
			try {
				ep = object.showPlan(sc, fakes);
			} catch (PEException pe) {
				throw new SchemaException(Pass.PLANNER, "Unable to build show plan", pe);
			}
			ArrayList<ResultRow> planRows = new ArrayList<ResultRow>();
			ep.explain(sc, planRows, opts);
			for(ResultRow rr : planRows) {
				ResultRow nr = new ResultRow();
				nr.addResultColumn(dbName);
				nr.addResultColumn(pck.getShrunk());
				nr.getRow().addAll(rr.getRow());	
				rows.add(nr);
			}
		}
		
	
	}
	
}
