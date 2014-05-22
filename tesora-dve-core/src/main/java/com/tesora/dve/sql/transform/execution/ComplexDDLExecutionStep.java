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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;

public class ComplexDDLExecutionStep extends CatalogModificationExecutionStep {

	private DDLCallback cb;
	private Boolean commitOverride = null;
	
	public ComplexDDLExecutionStep(PEDatabase db, PEStorageGroup tsg,
			Persistable<?, ?> root, Action act, DDLCallback callback) {
		super(db, tsg, root, act);
		cb = callback;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		QueryStepDDLGeneralOperation qso = null;
		if (cb instanceof NestedOperationDDLCallback) {
			qso = new QueryStepDDLNestedOperation(getPersistentDatabase(),(NestedOperationDDLCallback)cb);
		} else {
			qso = new QueryStepDDLGeneralOperation(getPersistentDatabase());
			qso.setEntities(cb);
		}
		if (commitOverride != null)
			qso = qso.withCommitOverride(false);
		addStep(sc,qsteps,qso);

	}

	public ComplexDDLExecutionStep withCommitOverride(boolean v) {
		commitOverride = v;
		return this;
	}

	@Override
	public CacheInvalidationRecord getCacheInvalidation(SchemaContext sc) {
		return cb.getInvalidationRecord();
	}
	

}
