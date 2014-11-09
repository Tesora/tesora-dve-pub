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

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.ConnectionValues;
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
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValues cv)
			throws PEException {
		QueryStepDDLGeneralOperation qso = null;
		StorageGroup sg = getStorageGroup(sc,cv);
		if (cb instanceof NestedOperationDDLCallback) {
			qso = new QueryStepDDLNestedOperation(sg,getPersistentDatabase(),(NestedOperationDDLCallback)cb);
		} else {
			qso = new QueryStepDDLGeneralOperation(sg,getPersistentDatabase());
			qso.setEntities(cb);
		}
		if (commitOverride != null)
			qso = qso.withCommitOverride(false);
		qsteps.add(qso);
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
