// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import org.apache.log4j.Logger;

public enum MyLogEventType {
	// MySql log events
	START_EVENT_V3((byte) 0x01),
	QUERY_EVENT((byte) 0x02),
	STOP_EVENT((byte) 0x03),
	ROTATE_EVENT((byte) 0x04),
	INTVAR_EVENT((byte) 0x05),
	LOAD_EVENT((byte) 0x06),
	SLAVE_EVENT((byte) 0x07),
	CREATE_FILE_EVENT((byte) 0x08),
	APPEND_BLOCK_EVENT((byte) 0x09),
	EXEC_LOAD_EVENT((byte) 0x0A),
	DELETE_FILE_EVENT((byte) 0x0B),
	NEW_LOAD_EVENT((byte) 0x0C),
	RAND_EVENT((byte) 0x0D),
	USER_VAR_EVENT((byte) 0x0E),
	FORMAT_DESCRIPTION_EVENT((byte) 0x0F),
	XID_EVENT((byte) 0x10),
	BEGIN_LOAD_QUERY_EVENT((byte) 0x11),
	EXECUTE_LOAD_QUERY_EVENT((byte) 0x12),
	TABLE_MAP_EVENT((byte) 0x13),
	INCIDENT_EVENT((byte) 0x1A),
	HEARTBEAT_LOG_EVENT((byte) 0x1B),
    

	// These event numbers were used for 5.1.0 to 5.1.15 and are
    // therefore obsolete.
	PRE_GA_WRITE_ROWS_EVENT((byte) 0x14),
	PRE_GA_UPDATE_ROWS_EVENT((byte) 0x15),
	PRE_GA_DELETE_ROWS_EVENT((byte) 0x16),

	// These event numbers are used from 5.1.16 and forward
	WRITE_ROWS_EVENT((byte) 0x17),
	UPDATE_ROWS_EVENT((byte) 0x18),
	DELETE_ROWS_EVENT((byte) 0x19),

	UNKNOWN_EVENT((byte) 0x00);

	private static final Logger logger = Logger.getLogger(MyLogEventType.class);
	
	private final byte msgTypeasByte;
	
	private MyLogEventType(byte b) {
		msgTypeasByte = b;
	}
	
	public static MyLogEventType fromByte(byte b) {
		for (MyLogEventType mt : values()) {
			if (mt.msgTypeasByte == b) {
				return mt;
			}
		}
	
		logger.error("Unknown log event type requested: " + b );
		return UNKNOWN_EVENT;
	}
	
	public byte getByteValue() {
		return msgTypeasByte;
	}
}
