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

import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.UnaryProcedure;

public interface HasPlanning {

	void display(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containingPlan, List<String> buf, String indent, EmitOptions opts);
	
	void explain(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containingPlan, List<ResultRow> rows, ExplainOptions opts);
	
	Long getlastInsertId(ValueManager vm, SchemaContext sc, ConnectionValues cv);
	
	Long getUpdateCount(SchemaContext sc, ConnectionValues cv);

	boolean useRowCount();
	
	void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, 
			SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containingPlan) throws PEException;
	
	ExecutionType getExecutionType();
	
	CacheInvalidationRecord getCacheInvalidation(SchemaContext sc);

	void prepareForCache();
	
	void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc);	
}
