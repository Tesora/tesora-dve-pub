// OS_STATUS: public
package com.tesora.dve.persist;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public interface PersistProvider {

	public Long insert(PersistedInsert pi) throws PEException;
	
	public List<ResultRow> query(DMLStatement dmls) throws PEException;
	
}
