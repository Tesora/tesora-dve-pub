// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;


import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepAdhocResultSetOperation;
import com.tesora.dve.queryplan.QueryStepShowCatalogEntityOperation;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;

public class DDLQueryExecutionStep extends ExecutionStep {

	private List<CatalogEntity> queriedEntities;
	private IntermediateResultSet populatedResults;
	private String tag;
	private boolean pluralForm;
	private boolean tenant;
	
	public DDLQueryExecutionStep(String tag, List<CatalogEntity> ents, boolean plural, boolean isTenant) {
		super(null, null, ExecutionType.DDLQUERY);
		queriedEntities = ents;
		this.tag = tag;
		pluralForm = plural;
		tenant = isTenant;
	}

	public DDLQueryExecutionStep(String tag, IntermediateResultSet results) {
		super(null, null, ExecutionType.DDLQUERY);
		queriedEntities = null;
		populatedResults = results;
		this.tag = tag;
		pluralForm = false;
		tenant = false;
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		if (populatedResults != null) {
			addStep(sc,qsteps,new QueryStepAdhocResultSetOperation(populatedResults));
			return;
		} else {

			boolean extensions = SchemaVariables.isShowMetadataExtensions(sc);
			addStep(sc,qsteps,new QueryStepShowCatalogEntityOperation(queriedEntities, new CatalogQueryOptions(extensions, pluralForm, tenant)));
		}
	}

	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
	}

	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		buf.add(indent + getExecutionType().name() + " schema query " + tag);
	}


}
