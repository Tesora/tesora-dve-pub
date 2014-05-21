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
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;
import com.tesora.dve.sql.util.UnaryProcedure;

public class ExecutionSequence extends ExecutionStep {

	protected LinkedList<HasPlanning> steps;  //NOPMD
	// backward links, good for getting to the plan during construction
	protected ExecutionPlan plan;
	
	public ExecutionSequence(ExecutionPlan p) {
		super(null,null,ExecutionType.SEQUENCE);
		steps = new LinkedList<HasPlanning>();
		plan = p;
	}
	
	public void append(HasPlanning es) {
		if (es == null) return;
		steps.add(es);
	}
	
	public ExecutionPlan getPlan() {
		return plan;
	}

	protected String sequenceName() {
		return "SEQUENCE";
	}
	
	@Override
	public void display(final SchemaContext sc, final List<String> sb, String indent, final EmitOptions opts) {
		if (steps.isEmpty()) return;
		sb.add(indent + sequenceName() + " {");
		final String subindent = indent + "  ";
		apply(true,new UnaryProcedure<HasPlanning>() {

			@Override
			public void execute(HasPlanning object) {
				object.display(sc, sb, subindent, opts);
			}
			
		});
		sb.add(indent + "}");
	}
	
	@Override
	public void explain(final SchemaContext sc, final List<ResultRow> rows, final ExplainOptions opts) {
		if (steps.isEmpty()) return;
		apply(true,new UnaryProcedure<HasPlanning>() {

			@Override
			public void execute(HasPlanning object) {
				object.explain(sc, rows, opts);
			}
			
		});
	}
	
	public List<HasPlanning> getSteps() {
		return steps;
	}
	
	public void setSteps(List<HasPlanning> other) {
		steps.addAll(other);
	}
	
	public HasPlanning getLastStep() {
		if (steps.isEmpty()) return null;
		return steps.getLast();
	}
	
	@Override
	public Long getlastInsertId(final ValueManager vm, final SchemaContext sc) {
		return getLastExisting(new UnaryFunction<Long, HasPlanning>() {

			@Override
			public Long evaluate(HasPlanning object) {
				return object.getlastInsertId(vm,sc);
			}
			
		});
	}

	@Override
	public Long getUpdateCount(final SchemaContext sc) {
		final Long[] summed = new Long[1];
		apply(true, new UnaryProcedure<HasPlanning>() {

			@Override
			public void execute(HasPlanning object) {
				Long uc = object.getUpdateCount(sc);
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
		return any(new UnaryPredicate<HasPlanning>() {

			@Override
			public boolean test(HasPlanning object) {
				return object.useRowCount();
			}
			
		});
	}
	
	public void apply(boolean ascending, UnaryProcedure<HasPlanning> f) {
		if (steps.isEmpty()) return;
		for(Iterator<HasPlanning> iter = (ascending ? steps.iterator() : steps.descendingIterator()); iter.hasNext();) {
			f.execute(iter.next());
		}
	}
	
	public boolean any(UnaryPredicate<HasPlanning> f) {
		if (steps.isEmpty()) return false;
		return Functional.any(steps, f);
	}
	
	public <T> T getLastExisting(UnaryFunction<T, HasPlanning> f) {
		if (steps.isEmpty()) return null;
		for(Iterator<HasPlanning> iter = steps.descendingIterator(); iter.hasNext();) {
			T any = f.evaluate(iter.next());
			if (any != null) return any;
		}
		return null;
	}

	@Override
	public void prepareForCache() {
		apply(true, new UnaryProcedure<HasPlanning>() {

			@Override
			public void execute(HasPlanning object) {
				object.prepareForCache();
			}
			
		});
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		if (steps.isEmpty()) return;
		for(HasPlanning hp : steps)
			hp.schedule(opts, qsteps, projection, sc);
	}

	@Override
	public ExecutionType getExecutionType() {
		return null;
	}
	
	public int getRedistributionStepCount(final SchemaContext sc) {
		final AtomicLong counter = new AtomicLong(0);
		this.analyze(new UnaryProcedure<DirectExecutionStep>() {
			@Override
			public void execute(final DirectExecutionStep object) {
				if (object instanceof ProjectingExecutionStep) {
					final ProjectingExecutionStep step = (ProjectingExecutionStep) object;
					if (step.getRedistTable(sc) != null) {
						counter.incrementAndGet();
					}
				}
			}

		});

		return counter.intValue();
	}

	public void analyze(UnaryProcedure<DirectExecutionStep> f) {
		if (steps.isEmpty()) return;
		for(HasPlanning hp : steps) {
			if (hp instanceof DirectExecutionStep) {
				f.execute((DirectExecutionStep)hp);
			} else if (hp instanceof ParallelExecutionStep) {
				((ParallelExecutionStep)hp).analyze(f);
			}
		}
	}

	@Override
	public CacheInvalidationRecord getCacheInvalidation(SchemaContext sc) {
		for(HasPlanning hp : steps) {
			CacheInvalidationRecord any = hp.getCacheInvalidation(sc);
			if (any != null)
				return any;
		}
		return null;
	}

	@Override
	public void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc) {
		if (steps.isEmpty()) return;
		for(HasPlanning hp : steps)
			hp.visitInExecutionOrder(proc);
	}

}
