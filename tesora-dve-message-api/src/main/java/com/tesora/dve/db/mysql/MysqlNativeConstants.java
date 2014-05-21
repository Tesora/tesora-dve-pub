// OS_STATUS: public
package com.tesora.dve.db.mysql;

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

public class MysqlNativeConstants {
	static final String DB_CHAR_SET = "utf8";
	static final String DB_ZERO_DATE_BEHAVIOUR = "round";

	static final String MYSQL_IDENTIFIER_QUOTE_CHAR = "`";
	static final String MYSQL_LITERAL_QUOTE_CHAR = "'";

	public static final int MAX_PACKET_SIZE = 16777216;
	public static final int MAX_DECIMAL_PRECISION = 65;

	// Character sets
	public static final byte MYSQL_CHARSET_LATIN1 = 8;
	public static final byte MYSQL_CHARSET_UTF8 = 33;
	public static final byte MYSQL_CHARSET_BINARY = 63;

	// Flags on the Field Packet (used by PEMysqlProtocolConverter)
	public static final short FLDPKT_FLAG_NONE=0;
	public static final short FLDPKT_FLAG_NOT_NULL=1;
    public static final short FLDPKT_FLAG_PRI_KEY=2;
    public static final short FLDPKT_FLAG_UNIQUE_KEY=4;
    public static final short FLDPKT_FLAG_MULTIPLE_KEY=8;
    public static final short FLDPKT_FLAG_BLOB=16;
    public static final short FLDPKT_FLAG_UNSIGNED=32;
    public static final short FLDPKT_FLAG_ZEROFILL=64;
    public static final short FLDPKT_FLAG_BINARY=128;
    public static final short FLDPKT_FLAG_ENUM=256;
    public static final short FLDPKT_FLAG_AUTO_INCREMENT=512;
    public static final short FLDPKT_FLAG_TIMESTAMP=1024;
    public static final short FLDPKT_FLAG_SET=2048;
    public static final short FLDPKT_FLAG_NUM=4096;
    public static final short FLDPKT_FLAG_ON_UPDATE_NOW=8192;
    public static final short FLDPKT_FLAG_PART_KEY=16384;

    public static final String MYSQL_CHARSET_ENCODING = "US-ASCII";
    
    // Default date formats
    public static final String MYSQL_DATE_FORMAT = "yyyy-MM-dd";
    public static final String MYSQL_TIME_FORMAT = "HH:mm:ss";
    public static final String MYSQL_TIME_FORMAT_MS = "HH:mm:ss.S";
    public static final String MYSQL_DATETIME_FORMAT = MYSQL_DATE_FORMAT + " " + MYSQL_TIME_FORMAT; 
    public static final String MYSQL_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
	public static final String[] MYSQL_DATE_FORMAT_PATTERNS = { 
		MYSQL_DATE_FORMAT, 
		MYSQL_TIME_FORMAT,
		MYSQL_TIME_FORMAT_MS,
		MYSQL_TIMESTAMP_FORMAT, 
		MYSQL_DATETIME_FORMAT
		};    
	
	// Constants for Status Variables used in MyStatisticsResponse
	public static final String MYSQL_THREAD_COUNT = "Threads_connected";
	public static final String MYSQL_QUESTIONS = "Questions";
	public static final String MYSQL_UPTIME = "Uptime";
	public static final String MYSQL_SLOW_QUERIES = "Slow_queries";
	// other Status variable constants
	public static final String MYSQL_CONNECTIONS = "Connections";
	public static final String MYSQL_CONNECTIONS_MAX_CONCUR = "Connections_max_concurrent";
	public static final String MYSQL_ABORTED_CONNECTS = "Aborted_connects";
	public static final String MYSQL_ABORTED_CLIENTS = "Aborted_clients";
	
}