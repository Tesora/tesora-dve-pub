// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;


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
