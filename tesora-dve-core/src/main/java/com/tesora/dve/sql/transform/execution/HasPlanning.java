// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.UnaryProcedure;

public interface HasPlanning {

	void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts);
	
	void explain(SchemaContext sc, List<ResultRow> rows, ExplainOptions opts);
	
	Long getlastInsertId(ValueManager vm, SchemaContext sc);
	
	Long getUpdateCount(SchemaContext sc);

	boolean useRowCount();
	
	void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc) throws PEException;
	
	ExecutionType getExecutionType();
	
	CacheInvalidationRecord getCacheInvalidation(SchemaContext sc);

	void prepareForCache();
	
	void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc);
	
}
