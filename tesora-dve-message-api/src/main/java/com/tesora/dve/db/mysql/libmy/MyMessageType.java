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

import org.apache.log4j.Logger;

public enum MyMessageType {
		// Mysql Native Request message types
	    LOGIN_REQUEST((byte) 0xC0),

	    // Used for replication
	    REPL_EVENT_RESPONSE((byte) 0x00),

	    // The COM_ request types correspond to MySQL Command Packets
	    COM_QUIT_REQUEST((byte) 0x01),
	    COM_INIT_DB_REQUEST((byte) 0x02),
	    COM_QUERY_REQUEST((byte) 0x03),
	    COM_FIELD_LIST_REQUEST((byte) 0x04),
	    COM_STATISTICS_REQUEST((byte) 0x09),
	    COM_PROCESS_INFO_REQUEST((byte) 0x0a),
	    COM_PING_REQUEST((byte) 0x0e),
	    COM_SET_OPTION_REQUEST((byte) 0x1b),
	    COM_BINLOG_DUMP_REQUEST((byte) 0x12),
	    COM_REGISTER_SLAVE_REQUEST((byte) 0x15),
	    COM_STMT_PREPARE_REQUEST((byte) 0x16),
	    COM_STMT_EXECUTE_REQUEST((byte) 0x17),
	    COM_STMT_SEND_LONG_DATA_REQUEST((byte) 0x18),
	    COM_STMT_CLOSE_REQUEST((byte) 0x19),
	    COM_STMT_RESET_REQUEST((byte) 0x1a),
	    
	    // Mysql Native Response message types
	    SERVER_GREETING_RESPONSE((byte) 0xD0),
	    OK_RESPONSE((byte) 0xD1),
	    ERROR_RESPONSE((byte) 0xD2),
	    RESULTSET_RESPONSE((byte) 0xD3),
	    FIELDPKT_RESPONSE((byte) 0xD4),
	    ROWDATA_RESPONSE((byte) 0xD5),
	    EOFPKT_RESPONSE((byte) 0xD6),
	    FIELDLIST_RESPONSE((byte) 0xD7),
	    FIELDPKTFIELDLIST_RESPONSE((byte) 0xD8),
	    PREPAREOK_RESPONSE((byte) 0xD9),
	    SERVER_GREETING_ERROR_RESPONSE((byte) 0xDA),
	    
	    // not an official type
	    LOCAL_INFILE_REQUEST((byte) 0xFA),
	    LOCAL_INFILE_DATA((byte) 0xFB),
	    
	    UNKNOWN((byte) 0xFF);

	private static final Logger logger = Logger.getLogger(MyMessageType.class);

	private final byte msgTypeasByte;

	private MyMessageType(byte b) {
		msgTypeasByte = b;
	}

	public static MyMessageType fromByte(byte b) {
		for (MyMessageType mt : values()) {
			if (mt.msgTypeasByte == b) {
				return mt;
			}
		}

		logger.error("Unknown message type requested: " + b );
		return UNKNOWN;
	}

	public byte getByteValue() {
		return msgTypeasByte;
	}

}
