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


import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryProcedure;

public class ParallelExecutionStep extends ExecutionStep {

	protected LinkedList<ExecutionSequence> parallel = new LinkedList<ExecutionSequence>();  //NOPMD

	public ParallelExecutionStep(Database<?> db, PEStorageGroup storageGroup) {
		super(db, storageGroup, ExecutionType.PARALLEL);
	}

	public void addSequence(ExecutionSequence seq) {
		if (seq.getSteps().isEmpty()) return;
		parallel.add(seq);
	}
	
	public void addStep(ExecutionStep es) {
		if (es instanceof ExecutionSequence) {
			addSequence((ExecutionSequence)es);
		} else if (es instanceof ParallelExecutionStep) {
			// all the sequences within the target can be put on our parallel
			ParallelExecutionStep child = (ParallelExecutionStep) es;
			parallel.addAll(child.getSequences());
		}
	}
	
	public List<ExecutionSequence> getSequences() {
		return parallel;
	}

	@Override
	public void getSQL(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, EmitOptions opts) {
	}
	
	@Override
	public void display(final SchemaContext sc, final ConnectionValuesMap cvm, final ExecutionPlan containing, final List<String> buf, String indent, final EmitOptions opts) {
		buf.add(indent + "PARALLEL {");
		final String subindent = indent + "  ";
		apply(true, new UnaryProcedure<ExecutionSequence>() {

			@Override
			public void execute(ExecutionSequence object) {
				object.display(sc, cvm,containing,buf, subindent,opts);
			}
			
		});
		buf.add(indent + "}");
	}


	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cvm, ExecutionPlan containing)
			throws PEException {
		for(ExecutionSequence es : parallel)
			es.schedule(opts, qsteps, projection, sc, cvm, containing);
	}

	@Override
	public void explain(final SchemaContext sc, final ConnectionValuesMap cvm, final ExecutionPlan containing, final List<ResultRow> rows, final ExplainOptions opts) {
		apply(true, new UnaryProcedure<ExecutionSequence>() {

			@Override
			public void execute(ExecutionSequence object) {
				object.explain(sc,cvm,containing,rows,opts);
			}
			
		});
	}
	
	@Override
	public Long getlastInsertId(final ValueManager vm, final SchemaContext sc, final ConnectionValues cv) {
		return getLastExisting(new UnaryFunction<Long, ExecutionSequence>() {

			@Override
			public Long evaluate(ExecutionSequence object) {
				return object.getlastInsertId(vm,sc,cv);
			}
			
		});
	}
	
	@Override
	public Long getUpdateCount(final SchemaContext sc, final ConnectionValues cv) {
		final Long[] summed = new Long[1];
		apply(true, new UnaryProcedure<ExecutionSequence>() {

			@Override
			public void execute(ExecutionSequence object) {
				Long uc = object.getUpdateCount(sc, cv);
				if (uc != null) {
					if (summed[0] == null)
						summed[0] = uc;
					else
						summed[0] = new Long(summed[0].longValue() + uc.longValue());
				}
			}
			
		});
		return summed[0];
	}

	@Override
	public boolean useRowCount() {
		return false;
	}
	
	
	public void apply(boolean ascending, UnaryProcedure<ExecutionSequence> proc) {
		if (parallel.isEmpty()) return;
		for(Iterator<ExecutionSequence> iter = (ascending ? parallel.iterator() : parallel.descendingIterator()); iter.hasNext();) {
			proc.execute(iter.next());
		}
	}
	
	public <T> T getLastExisting(UnaryFunction<T, ExecutionSequence> f) {
		if (parallel.isEmpty()) return null;
		for(Iterator<ExecutionSequence> iter = parallel.descendingIterator(); iter.hasNext();) {
			T any = f.evaluate(iter.next());
			if (any != null) return any;
		}
		return null;
	}

	public void analyze(final UnaryProcedure<DirectExecutionStep> f) {
		apply(true, new UnaryProcedure<ExecutionSequence>() {

			@Override
			public void execute(ExecutionSequence object) {
				object.analyze(f);
			}
			
		});
	}	
	
	@Override
	public CacheInvalidationRecord getCacheInvalidation(SchemaContext sc) {
		for(ExecutionSequence es : parallel) {
			CacheInvalidationRecord cir = es.getCacheInvalidation(sc);
			if (cir != null)
				return cir;
		}
		return null;
	}

	@Override
	public void prepareForCache() {
		apply(true, new UnaryProcedure<ExecutionSequence>() {

			@Override
			public void execute(ExecutionSequence object) {
				object.prepareForCache();
			}
			
		});
	}

	@Override
	public void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc) {
		for(ExecutionSequence es : parallel)
			es.visitInExecutionOrder(proc);		
	}
	
	@Override
	public void visitInTestVerificationOrder(UnaryProcedure<HasPlanning> proc) {
		for(ExecutionSequence es : parallel)
			es.visitInTestVerificationOrder(proc);		
	}

}
