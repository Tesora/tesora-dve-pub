// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLOperation;
import com.tesora.dve.queryplan.QueryStepDropTableOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;

public class DropTableExecutionStep extends SimpleDDLExecutionStep {
	List<Name> unknownTables;
	boolean ifExists;
	
	public DropTableExecutionStep(PEDatabase db, PEStorageGroup tsg,
			Persistable<?, ?> root, Action act, SQLCommand sql,
			List<CatalogEntity> deleteList, List<CatalogEntity> entityList,
			CacheInvalidationRecord invalidate, List<Name> unknownTables, boolean ifExists) {
		super(db, tsg, root, act, sql, deleteList, entityList, invalidate);
		this.unknownTables = unknownTables;
		this.ifExists = ifExists;
	}

	@Override
	protected QueryStepDDLOperation buildOperation(SchemaContext sc)
			throws PEException {
		QueryStepDropTableOperation qso = new QueryStepDropTableOperation(getPersistentDatabase(), sql, getCacheInvalidation(sc), unknownTables, ifExists);
		if (getCommitOverride() != null)
			return (QueryStepDropTableOperation) qso.withCommitOverride(getCommitOverride());
		return qso;
	}
}
