// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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

import com.tesora.dve.dbc.ServerDBConnection;

public class MyBinLogDAO {
	private static final String BINLOG_STATUS_TBL_NAME = "binlog_status";
	private static final String BINLOG_STATUS_CREATE_TBL = "CREATE TABLE " + BINLOG_STATUS_TBL_NAME + " (" +
			"master_host varchar(255) not null, " + 
			"last_filename varchar(255) not null, " +
			"last_position bigint not null, " + 
			"primary key (master_host) )";

	public boolean doesBinLogStatusTableExist(ServerDBConnection conn) throws SQLException {
		boolean exists = false;
		
		// check for binlog_info table existence
		ResultSet rs = conn.executeQuery("SHOW TABLES");
		try {
			while (rs.next()) {
				if (rs.getString(1).equals(BINLOG_STATUS_TBL_NAME)) {
					exists = true;
					break;
				}
			}
		} finally {
			rs.close();
		}
		
		return exists;
	}
	
	public void createBinLogStatusTable(ServerDBConnection conn) throws SQLException {
		conn.execute(BINLOG_STATUS_CREATE_TBL);
	}

	public MyBinLogPosition getBinLogPosition(ServerDBConnection conn, String masterHost) throws SQLException {
		MyBinLogPosition blp = null;
		ResultSet rs = conn.executeQuery("SELECT * FROM " + BINLOG_STATUS_TBL_NAME + 
											" WHERE master_host ='" + masterHost + "'");
		
		try {
			if ( rs.next() ) {
				blp = new MyBinLogPosition(rs.getString(1), rs.getString(2), rs.getLong(3));
			} else {
				throw new SQLException("Could not find binlog_status information for " + masterHost);
			}
		} finally {
			rs.close();
		}
		
		return blp;
	}
	
	public void updateBinLogPosition(ServerDBConnection conn, MyBinLogPosition blp) throws SQLException {

		int rowsAffected = conn.executeUpdate("UPDATE " + BINLOG_STATUS_TBL_NAME + " SET last_filename = '" + blp.getFileName() +
				"', last_position=" + blp.getPosition() + " WHERE master_host='" +  blp.getMasterHost() + "'");
		
		if ( rowsAffected == 0 ) {
			conn.executeUpdate("INSERT INTO " + BINLOG_STATUS_TBL_NAME + 
								" (master_host,last_filename,last_position) VALUES " +
								"('" + blp.getMasterHost() + "','" + blp.getFileName() + "'," + blp.getPosition() + ")");
		}
	}
	
}
