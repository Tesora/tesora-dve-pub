// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

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


public final class MyProtocolDefs {

	public static final String MY_PROTOCONV_PREFIX="PEMysqlProtocolConverter";
	public static final String MY_PROTOCONV_URL_KEY=MY_PROTOCONV_PREFIX + ".url";
	public static final String MY_PROTOCONV_DRIVERCLASS_KEY=MY_PROTOCONV_PREFIX + ".driverClass";
	public static final String MY_PROTOCONV_TIMEOUT_KEY=MY_PROTOCONV_PREFIX + ".msgTimeout";

	
	// Server Side definitions
	public static final byte MYSQL_PROTOCOL_VERSION=10;
	
	// Server Status definitions
    public static final short SERVER_STATUS_IN_TRANS = 1;
    public static final short SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode

    // to prevent instantiation
    private MyProtocolDefs () {};
    
}
