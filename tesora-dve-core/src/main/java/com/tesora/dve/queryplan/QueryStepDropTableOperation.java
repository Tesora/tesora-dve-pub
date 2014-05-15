// OS_STATUS: public
package com.tesora.dve.queryplan;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;

public class QueryStepDropTableOperation extends QueryStepDDLOperation {
	
	List<Name> unknownTables;
	boolean ifExists;
	
	public QueryStepDropTableOperation(PersistentDatabase execCtxDBName,
			SQLCommand command, CacheInvalidationRecord invalidationRecord, List<Name> unknownTables, boolean ifExists) {
		super(execCtxDBName, command, invalidationRecord);
		this.unknownTables = unknownTables;
		this.ifExists = ifExists;
	}

	@Override
	protected void postCommitAction(CatalogDAO c) throws PEException {
		if (unknownTables != null && unknownTables.size()>0) {
			if (!ifExists) {
				throw new PEException("Unknown table '" + StringUtils.join(unknownTables, ",") + "'");
			} else {
				// set warning count to number of unknown tables if we only could...
			}
		}
	}
}
