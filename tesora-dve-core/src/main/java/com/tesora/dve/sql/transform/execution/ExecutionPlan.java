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
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.raw.ExecToRawConverter;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.CachedPlan;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.util.UnaryProcedure;

public class ExecutionPlan implements HasPlanning {

	private static final Logger logger = Logger.getLogger( ExecutionPlan.class );
	
	private ExecutionSequence steps;
	private ProjectionInfo projection;
	// cacheable only if true
	private Boolean cacheable;
	@SuppressWarnings("unused")
	private CachedPlan owner;
	
	private final ValueManager values;
	
	// com_* stats are populated via this when there is a plan cache hit
	private final StatementType originalStatementType;
	
	private boolean isEmptyPlan = false;
	
	public ExecutionPlan(ProjectionInfo pi, ValueManager vm, StatementType stmtType) {
		steps = new ExecutionSequence(this);
		projection = pi;
		values = vm;
		originalStatementType = stmtType;
	}
	
	public List<QueryStepOperation> schedule(ExecutionPlanOptions opts, SSConnection connection, SchemaContext sc) throws PEException {
		List<QueryStepOperation> buf = new ArrayList<QueryStepOperation>();
		schedule(opts, buf,projection,sc);
		Long lastInsertId = steps.getlastInsertId(values,sc);
		if (lastInsertId != null)
			connection.setLastInsertedId(lastInsertId.longValue());
		else 
			connection.setLastInsertedId(0);
		return buf;
	}

	public ExecutionSequence getSequence() {
		return steps;
	}
	
	public void display(SchemaContext sc, PrintStream ps, EmitOptions opts) {
		display(sc, ps,"",opts);
	}
	
	public void display(SchemaContext sc, PrintStream ps, String cntxt, EmitOptions opts) {
		ps.println();
		boolean prepared = values.getNumberOfParameters() > 0 && !values.hasPassDownParams();
		ps.println("--- Execution Plan " + cntxt + (prepared ? " (prepared stmt) " : "") + " ---");
		ArrayList<String> buf = new ArrayList<String>();
		steps.display(sc, buf,"  ",(opts == null ? EmitOptions.NONE.addMultilinePretty("  ") : opts.addMultilinePretty("  "))); 
		for(String s : buf)
			ps.println(s);
		ps.println("--- End Execution Plan ---");
	}
	
	public void logPlan(SchemaContext sc, String cntxt, EmitOptions opts) {
		if (!logger.isInfoEnabled())
			return;
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(buf);
		display(sc, ps,cntxt,opts);
		ps.flush();
		ps.close();
		logger.info(buf.toString());
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
	
	private IntermediateResultSet generateExplain(SchemaContext sc, ExplainOptions opts) {
		ColumnSet cs = new ColumnSet();
		addExplainColumnHeaders(cs);
		if (opts.isStatistics()) 
			StepExecutionStatistics.addColumnHeaders(cs);
		List<ResultRow> rows = new ArrayList<ResultRow>();
		explain(sc,rows,opts);
		return new IntermediateResultSet(cs,rows);
	}
	
	
	public DDLQueryExecutionStep generateExplain(SchemaContext sc, Statement sp, String origSQL) {
		boolean standard = true;
		if (sp.getExplain() == null)
			standard = true;
		else {
			standard = !sp.getExplain().isRaw();
		}
		if (standard)
			return new DDLQueryExecutionStep("explain", generateExplain(sc,sp.getExplain()));
		else {
			return new DDLQueryExecutionStep("rawexplain",ExecToRawConverter.convertForRawExplain(sc, this, sp, origSQL));
		}
	}
	
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
	}

	@Override
	public Long getlastInsertId(ValueManager vm, SchemaContext sc) {
		return steps.getlastInsertId(vm, sc);
	}

	@Override
	public Long getUpdateCount(SchemaContext sc) {
		return steps.getUpdateCount(sc);
	}
	
	@Override
	public boolean useRowCount() {
		return steps.useRowCount();
	}

	@Override
	public void explain(SchemaContext sc, List<ResultRow> rows,
			ExplainOptions opts) {
		steps.explain(sc,  rows, opts);		
	}


	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		ArrayList<QueryStepOperation> buf = new ArrayList<QueryStepOperation>();
		steps.schedule(opts, buf,projection, sc);
		if (buf.isEmpty()) return;

		// the plan is represented by a tree - of which the sequence is the root.
		// convert the tree such that the last execution step is the single query step
		// and all others are dependent steps.
		QueryStepOperation end = buf.remove(buf.size() - 1);
		for(QueryStepOperation qs : buf) {
			end.addRequirement(qs);
		}
		qsteps.add(end);
	}

	@Override
	public ExecutionType getExecutionType() {
		return null;
	}
	
	public void setCacheable(boolean v) {
		// i.e. peg not cacheable
		if (Boolean.FALSE.equals(cacheable)) return;
		else if (!v) cacheable = false;
		else cacheable = v;
	}
	
	public boolean isCacheable() {
		return Boolean.TRUE.equals(cacheable);
	}

	public void setOwningCache(CachedPlan cp) {
		owner = cp;
		values.setFrozen();
		prepareForCache();
	}
	
	@Override
	public void prepareForCache() {
		steps.prepareForCache();
	}
	
	public ValueManager getValueManager() {
		return values;
	}	
	
	public ProjectionInfo getProjectionInfo() {
		return projection;
	}
	
	public StatementType getStatementType() {
		return originalStatementType;
	}

	public boolean isEmptyPlan() {
		return isEmptyPlan;
	}

	public void setIsEmptyPlan(boolean isEmptyPlan) {
		this.isEmptyPlan = isEmptyPlan;
	}

	@Override
	public void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc) {
		steps.visitInExecutionOrder(proc);		
	}

}
