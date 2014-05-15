// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLOperation;
import com.tesora.dve.queryplan.QueryStepExternalServiceOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;

public class ExternalServiceExecutionStep extends SimpleDDLExecutionStep {

	public ExternalServiceExecutionStep(PEDatabase db, PEStorageGroup tsg,
			Persistable<?, ?> root, Action act, SQLCommand sql,
			List<CatalogEntity> deleteList, List<CatalogEntity> entityList, CacheInvalidationRecord invalidate) {
		super(db, tsg, root, act, sql, deleteList, entityList, invalidate);
	}

	@Override
	protected QueryStepDDLOperation buildOperation(SchemaContext sc) throws PEException {
		return new QueryStepExternalServiceOperation(getPersistentDatabase(), sql,
				action, (PEExternalService) rootEntity);
	}

}
