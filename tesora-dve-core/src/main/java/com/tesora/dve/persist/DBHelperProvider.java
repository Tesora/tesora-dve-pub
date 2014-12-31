package com.tesora.dve.persist;

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

}
