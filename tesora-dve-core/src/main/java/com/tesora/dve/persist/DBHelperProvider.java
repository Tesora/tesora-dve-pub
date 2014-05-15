// OS_STATUS: public
package com.tesora.dve.persist;

import java.sql.SQLException;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public class DBHelperProvider implements PersistProvider {

	DBHelper helper;
	
	public DBHelperProvider(DBHelper h) {
		helper = h;
	}
	
	@Override
	public Long insert(PersistedInsert pi) throws PEException {
		String sql = pi.getSQL();
		try {
			helper.executeQuery(sql);
		} catch (SQLException sqle) {
			throw new PEException("Unable to execute: '" + sql + "'",sqle);
		}
		long any = helper.getLastInsertID();
		if (any > 0)
			return new Long(any);
		return null;
	}

	@Override
	public List<ResultRow> query(DMLStatement dmls) throws PEException {
		return null;
	}

}
