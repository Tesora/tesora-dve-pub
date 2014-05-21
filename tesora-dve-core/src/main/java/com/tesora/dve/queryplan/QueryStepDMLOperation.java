// OS_STATUS: public
package com.tesora.dve.queryplan;

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


import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.sql.transform.execution.StepExecutionStatistics;

public abstract class QueryStepDMLOperation extends QueryStepOperation {

	StepExecutionStatistics statistics;
	long before;
	
	protected final PersistentDatabase database;
	
	protected QueryStepDMLOperation(PersistentDatabase pdb) {
		this.statistics = null;
		this.database = pdb;
	}
	
	public void setStatistics(StepExecutionStatistics stats) {
		this.statistics = stats;
	}
	
	protected void beginExecution() {
		if (this.statistics == null) return;
		before = System.nanoTime();
	}
	
	protected void endExecution(long rows) {
		if (this.statistics == null) return;
		this.statistics.onExecution(System.nanoTime() - before, rows);
	}
	
	@Override
	public final PersistentDatabase getContextDatabase() {
		return database;
	}
	
}
