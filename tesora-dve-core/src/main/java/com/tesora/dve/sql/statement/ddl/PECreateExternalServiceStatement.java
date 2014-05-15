// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.transform.execution.ExternalServiceExecutionStep;

public class PECreateExternalServiceStatement extends
		PECreateStatement<PEExternalService, ExternalService> {

	public PECreateExternalServiceStatement(
			Persistable<PEExternalService, ExternalService> targ,
			boolean peOnly, String tag, boolean exists) {
		super(targ, peOnly, tag, exists);
	}

	@Override
	protected CatalogModificationExecutionStep buildStep(SchemaContext pc) throws PEException {
		if (!alreadyExists)
			return new ExternalServiceExecutionStep(getDatabase(pc),
					getStorageGroup(pc), getRoot(), getAction(), getSQLCommand(pc),
					getDeleteObjects(pc), getCatalogObjects(pc), CacheInvalidationRecord.GLOBAL);
		else
			return null;
	}
}
