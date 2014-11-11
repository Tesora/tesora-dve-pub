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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.raw.ExecToRawConverter;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryProcedure;

public abstract class ExecutionPlan implements HasPlanning {

	private static final Logger logger = Logger.getLogger( RootExecutionPlan.class );
		
	protected final ExecutionSequence steps;
	protected final ValueManager values;
	protected final ListSet<ExecutionPlan> nested;
	
	public ExecutionPlan(ValueManager valueManager) {
		this.values = valueManager;
		this.nested = new ListSet<ExecutionPlan>();
		this.steps = new ExecutionSequence(this);
	}
	
	public ExecutionSequence getSequence() {
		return steps;
	}
	
	public abstract boolean isRoot();

	public abstract void setCacheable(boolean v);
	public abstract boolean isCacheable();

	public ListSet<ExecutionPlan> getNestedPlans() {
		return nested;
	}
	
	public void addNestedPlan(ExecutionPlan ep) {
		nested.add(ep);
	}
	
	public ValueManager getValueManager() {
		return values;
	}
	
	@Override
	public void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc) {
		steps.visitInExecutionOrder(proc);		
	}

	@Override
	public void prepareForCache() {
		steps.prepareForCache();
	}
	
	@Override
	public ExecutionType getExecutionType() {
		return null;
	}

	// the plan is represented by a tree - of which the sequence is the root.
	// convert the tree such that the last execution step is the single query step
	// and all others are dependent steps.
	protected static QueryStepOperation collapseOperationList(List<QueryStepOperation> ops) {
		QueryStepOperation end = ops.remove(ops.size() - 1);
		for(QueryStepOperation qs : ops)
			end.addRequirement(qs);
		return end;
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cv, ExecutionPlan currentPlan)
			throws PEException {
		ArrayList<QueryStepOperation> buf = new ArrayList<QueryStepOperation>();
		steps.schedule(opts, buf,projection, sc, cv, this);
		if (buf.isEmpty()) return;

		qsteps.add(collapseOperationList(buf));
	}

	@Override
	public void display(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan currentPlan,
			List<String> buf, String indent, EmitOptions opts) {
		steps.display(sc, cv, this, buf,"  ",(opts == null ? EmitOptions.NONE.addMultilinePretty("  ") : opts.addMultilinePretty("  "))); 
	}

	@Override
	public Long getlastInsertId(ValueManager vm, SchemaContext sc, ConnectionValues cv) {
		return steps.getlastInsertId(vm, sc, cv);
	}

	@Override
	public Long getUpdateCount(SchemaContext sc, ConnectionValues cv) {
		return steps.getUpdateCount(sc, cv);
	}
	
	@Override
	public boolean useRowCount() {
		return steps.useRowCount();
	}

	@Override
	public void explain(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan currentPlan, List<ResultRow> rows,
			ExplainOptions opts) {
		steps.explain(sc,  cv, this, rows, opts);		
	}
	
	@Override
	public CacheInvalidationRecord getCacheInvalidation(final SchemaContext sc) {
		return steps.getCacheInvalidation(sc);
	}
	
	protected static final String[] basicExplainColumns = 
			new String[] { "Type", "Target_group", "Target_table", "Target_dist", "Target_index_hints", "Other", "SQL" };
	
	public static void addExplainColumnHeaders(ColumnSet cs) {
		for(String s : basicExplainColumns) 
			cs.addColumn(s,255,"varchar",Types.VARCHAR);		
	}
	
	private IntermediateResultSet generateExplain(SchemaContext sc, ConnectionValuesMap cv, ExplainOptions opts) {
		ColumnSet cs = new ColumnSet();
		addExplainColumnHeaders(cs);
		if (opts.isStatistics()) 
			StepExecutionStatistics.addColumnHeaders(cs);
		List<ResultRow> rows = new ArrayList<ResultRow>();
		explain(sc,cv, this, rows,opts);
		return new IntermediateResultSet(cs,rows);
	}
	
	
	public DDLQueryExecutionStep generateExplain(SchemaContext sc, ConnectionValuesMap cv, Statement sp, String origSQL) {
		boolean standard = true;
		if (sp.getExplain() == null)
			standard = true;
		else {
			standard = !sp.getExplain().isRaw();
		}
		if (standard)
			return new DDLQueryExecutionStep("explain", generateExplain(sc,cv,sp.getExplain()));
		else {
			return new DDLQueryExecutionStep("rawexplain",ExecToRawConverter.convertForRawExplain(sc, this, sp, origSQL));
		}
	}

	public void display(SchemaContext sc, ConnectionValuesMap cv, PrintStream ps, EmitOptions opts) {
		display(sc, cv, ps,"",opts);
	}
	
	public void display(SchemaContext sc, ConnectionValuesMap cv, PrintStream ps, String cntxt, EmitOptions opts) {
		ps.println();
		boolean prepared = values.getNumberOfParameters() > 0 && !values.hasPassDownParams();
		ps.println("--- Execution Plan " + cntxt + (prepared ? " (prepared stmt) " : "") + " ---");
		ArrayList<String> buf = new ArrayList<String>();
		steps.display(sc, cv, this, buf,"  ",(opts == null ? EmitOptions.NONE.addMultilinePretty("  ") : opts.addMultilinePretty("  "))); 
		for(String s : buf)
			ps.println(s);
		ps.println("--- End Execution Plan ---");
	}
	
	public void logPlan(SchemaContext sc, ConnectionValuesMap cv, String cntxt, EmitOptions opts) {
		if (!logger.isInfoEnabled())
			return;
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(buf);
		display(sc, cv,ps,cntxt,opts);
		ps.flush();
		ps.close();
		logger.info(buf.toString());
	}
	
	public void traverseExecutionPlans(UnaryProcedure<ExecutionPlan> proc) {
		// visit self
		proc.execute(this);
		// and now my nested plans
		for(ExecutionPlan ep : getNestedPlans()) {
			ep.traverseExecutionPlans(proc);
		}
	}

}
