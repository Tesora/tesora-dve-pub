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
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepFilterOperation.OperationFilter;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep.LateBindingOperationFilter;

/**
 * Set the update row count to the value obtained from a plan step.
 */
public abstract class LateBindingUpdateCountFilter extends LateBindingOperationFilter {
	
	/**
	 * Get the update count from a plan step.
	 */
	public static class LateBindingUpdateCounter extends LateBindingUpdateCountFilter {

		@Override
		public String describe() {
			return "LateBindingUpdateCounter";
		}

		@Override
		protected RuntimeUpdateCounter buildUpdateCounter() {
			return new RuntimeUpdateCounter(false);
		}

	}

	/**
	 * Get the update count from a plan step and accumulate with counts from any
	 * dependent steps.
	 */
	public static class LateBindingUpdateCountAccumulator extends LateBindingUpdateCountFilter {

		@Override
		public String describe() {
			return "LateBindingUpdateCountAccumulator";
		}

		@Override
		protected RuntimeUpdateCounter buildUpdateCounter() {
			return new RuntimeUpdateCounter(true);
		}

	}

	public class RuntimeUpdateCounter implements OperationFilter {

		private AtomicLong updateRowCount = new AtomicLong();
		private boolean updateCounterAccumulation = false;

		private RuntimeUpdateCounter(final boolean updateCounterAccumulation) {
			this.updateCounterAccumulation = updateCounterAccumulation;
		}

		public long incrementBy(final long increment) {
			return this.updateRowCount.addAndGet(increment);
		}

		public long getValue() {
			return this.updateRowCount.get();
		}

		public void setRuntimeUpdateCountAccumulation(final boolean enabled) {
			this.updateCounterAccumulation = enabled;
		}

		public boolean getRuntimeUpdateCountAccumulation() {
			return this.updateCounterAccumulation;
		}

		@Override
		public void filter(SSConnection ssCon, ColumnSet columnSet, List<ArrayList<String>> rowData, DBResultConsumer results) throws Throwable {
			this.updateRowCount.set(results.getUpdateCount());
		}

		@Override
		public String describe() {
			return "RuntimeUpdateCounter";
		}
		
		public String toString() {
			return "RuntimeUpdateCounter{" + getValue() + "}";
		}
	}

	public OperationFilter adapt(ExecutionPlanOptions opts, QueryStepOperation bindTo) throws PEException {
		if (opts.getRuntimeUpdateCounter() == null) {
			final RuntimeUpdateCounter counter = buildUpdateCounter();
			opts.setRuntimeUpdateCounter(counter);
			return counter;
		}

		throw new PEException("Cannot reset the " + describe() + ".");
	}

	protected abstract RuntimeUpdateCounter buildUpdateCounter();
}
