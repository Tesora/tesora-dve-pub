package com.tesora.dve.upgrade.versions;

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

import java.sql.ResultSet;
import java.sql.SQLException;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.Pair;

public abstract class ComplexCatalogVersion extends BasicCatalogVersion {

	public ComplexCatalogVersion(int version, boolean infoSchemaUpgrade) {
		super(version, infoSchemaUpgrade);
	}

	protected Pair<Long,Long> getGeneralBounds(DBHelper helper, String sql, String tabName) throws PEException {
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery(sql);
				rs = helper.getResultSet();
				rs.next();
				return new Pair<Long,Long>(rs.getLong(1), rs.getLong(2));
			} finally {
				if (rs != null) 
					rs.close();
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to determine bounds on " + tabName + " table",sqle);
		}		
	}
	
	protected Pair<Long, Long> getSimpleBounds(DBHelper helper, String tableName, String idcol) throws PEException {
		String sql = String.format("select min(%s), max(%s) from %s", idcol, idcol, tableName);
		return getGeneralBounds(helper, sql, tableName);
	}
	
	protected void execQuery(DBHelper helper, String sql) throws PEException {
		try {
			helper.executeQuery(sql);
		} catch (SQLException sqle) {
			throw new PEException("Unable to execute '" + sql + "'", sqle);
		}
	}

	protected void execQuery(DBHelper helper, String[] sql) throws PEException {
		for(String s : sql)
			execQuery(helper,s);
	}
	
	protected Object getSingleField(DBHelper helper, String sql) throws PEException {
		Object out = null;
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery(sql);
				rs = helper.getResultSet();
				if (rs.next())
					out = rs.getObject(1);
			} finally {
				if (rs != null)
					rs.close();
			}
		} catch (SQLException sqle) {
				throw new PEException("Unable to execute '" + sql + "'",sqle);
		}
		return out;
	}

}
