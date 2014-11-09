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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepAdhocResultSetOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepShowCatalogEntityOperation;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.variables.KnownVariables;

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
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValues cv)
			throws PEException {
		if (populatedResults != null) {
			qsteps.add(new QueryStepAdhocResultSetOperation(populatedResults));
			return;
		} else {

			boolean extensions =
					KnownVariables.SHOW_METADATA_EXTENSIONS.getValue(sc.getConnection().getVariableSource()).booleanValue();
			qsteps.add(new QueryStepShowCatalogEntityOperation(queriedEntities, new CatalogQueryOptions(extensions, pluralForm, tenant)));
		}
	}

	@Override
	public void getSQL(SchemaContext sc, ConnectionValues cv, List<String> buf, EmitOptions opts) {
	}

	@Override
	public void display(SchemaContext sc, ConnectionValues cv, List<String> buf, String indent, EmitOptions opts) {
		buf.add(indent + getExecutionType().name() + " schema query " + tag);
	}


}
