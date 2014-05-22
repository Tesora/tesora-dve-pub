package com.tesora.dve.comms.client.messages;

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
// NOPMD by doug on 18/12/12 7:34 AM


/**
 * Message type enumeration.
 */
public enum MessageType {

	// Client Request message types
    UNKNOWN((byte) 0x00),
    CONNECT_REQUEST((byte) 0x01),
    EXECUTE_REQUEST((byte) 0x02),
    GEOMETRY_REQUEST((byte) 0x03),
    FETCH_REQUEST((byte) 0x05),
    CLOSE_STATEMENT_REQUEST((byte) 0x06),
    BEGIN_TRANS_REQUEST((byte) 0x07),
    END_TRANS_REQUEST((byte) 0x08),
    CREATE_STATEMENT_REQUEST((byte) 0x09),
    DISCONNECT_REQUEST((byte) 0x0A),
    PRECONNECT_REQUEST((byte) 0x0B),
    ROLLBACK_TRANS_REQUEST((byte) 0x0C),
    GET_DATABASE_REQUEST((byte) 0x0D),
    GET_TRANS_STATUS_REQUEST((byte) 0x0E),
    PREPARE_REQUEST((byte) 0x0F),
    ADD_CONNECT_PARAMETERS_REQUEST((byte) 0x10),
    EXECUTE_PREPARED_REQUEST((byte) 0x11),
    LOAD_DATA_REQUEST((byte) 0x12),
    LOAD_DATA_BLOCK((byte) 0x13),
    
    // Client Response message types
    EXECUTE_RESPONSE((byte) 0x40),
    GEOMETRY_RESPONSE((byte) 0x41),
    FETCH_RESPONSE((byte) 0x42),
    GENERIC_RESPONSE((byte) 0x43),
    CREATE_STATEMENT_RESPONSE((byte) 0x44),
    MYSQL_PRECONNECT_RESPONSE((byte) 0x45),
    GET_DATABASE_RESPONSE((byte) 0x46),
    GET_TRANS_STATUS_RESPONSE((byte) 0x47),
    CONNECT_RESPONSE((byte) 0x48),
    PREPARE_RESPONSE((byte) 0x49),
    LOAD_DATA_RESPONSE((byte) 0x4A),
    
    // Metadata messages
    DBMETADATA_REQUEST((byte) 0xD0),
    DBMETADATA_RESPONSE((byte) 0xD1),

    // Framework Messages
    DIST_MAP_REQUEST((byte)0xE0),
    DIST_MAP_RESPONSE((byte)0xE1),
    DIST_NEW_GEN_REQUEST((byte)0xE2),
    
    WM_GET_WORKER_REQUEST((byte) 0xF0),
    WM_GET_WORKER_RESPONSE((byte) 0xF1),
    WM_RETURN_WORKER_REQUEST((byte) 0xF2),
    RESET_WORKER((byte) 0xF3),
    LOG_EVENT((byte) 0xF4),
    REDIST_ROW_REQUEST((byte)0xF5),
    REDIST_ROW_RESPONSE((byte)0xF6),
    W_EXECUTE_REQUEST((byte)0xF7),
    W_FETCH_REQUEST((byte)0xF8),
    W_TYPEINFO_REQUEST((byte)0xF9),
    W_PREPARE_REQUEST((byte)0xFA),
    W_COMMIT_REQUEST((byte)0xFB),
    W_ROLLBACK_REQUEST((byte)0xFC),
    W_CREATE_DB_REQUEST((byte)0xFD),
    REDIST_ROW_EXEC_REQUEST((byte)0xFE),
    
    // should be in the 0xFx series but that seems full
    W_GRANT_PRIVILEDGES_REQUEST((byte)0xC0),
	W_ALTER_DB_REQUEST((byte) 0xC1),
    
    STAT_REQUEST((byte)0xEA),
    STAT_RESPONSE((byte)0xEB), 
    SET_SESSION_VAR((byte)0xEC),
    WM_CLONE_WORKER_REQUEST((byte)0xED),
    
    NONE((byte) 0xFF);
    

	public static class MessageFactory {
		public static Class<? extends ClientMessage> getClass(MessageType cmt) {
			switch (cmt) {
			case PRECONNECT_REQUEST:
				return PreConnectRequest.class;
			case MYSQL_PRECONNECT_RESPONSE:
				return MysqlPreConnectResponse.class;
			case CONNECT_REQUEST:
				return ConnectRequest.class;
			case CONNECT_RESPONSE:
				return ConnectResponse.class;
			case DISCONNECT_REQUEST:
				return DisconnectRequest.class;
			case CREATE_STATEMENT_REQUEST:
				return CreateStatementRequest.class;
			case CREATE_STATEMENT_RESPONSE:
				return CreateStatementResponse.class;
			case EXECUTE_REQUEST:
				return ExecuteRequest.class;
			case EXECUTE_RESPONSE:
				return ExecuteResponse.class;
			case FETCH_REQUEST:
				return FetchRequest.class;
			case FETCH_RESPONSE:
				return FetchResponse.class;
			case DBMETADATA_REQUEST:
				return DBMetadataRequest.class;
			case DBMETADATA_RESPONSE:
				return DBMetadataResponse.class;
			case CLOSE_STATEMENT_REQUEST:
				return CloseStatementRequest.class;
			case GET_DATABASE_REQUEST:
				return GetDatabaseRequest.class;
			case GET_DATABASE_RESPONSE:
				return GetDatabaseResponse.class;
			case GENERIC_RESPONSE:
				return GenericResponse.class;

			case BEGIN_TRANS_REQUEST:
				return BeginTransactionRequest.class;
			case END_TRANS_REQUEST:
				return CommitTransactionRequest.class;
			case ROLLBACK_TRANS_REQUEST:
				return RollbackTransactionRequest.class;
			case GET_TRANS_STATUS_REQUEST:
				return GetTransactionStatusRequest.class;
			case GET_TRANS_STATUS_RESPONSE:
				return GetTransactionStatusResponse.class;

//			case WM_GET_WORKER_RESPONSE:
//				return GetWorkerResponse.class;
//			case REDIST_ROW_RESPONSE:
//				return RedistRowResponse.class;

//			case GEOMETRY_REQUEST:
//			case GEOMETRY_RESPONSE:
			default:
				throw new RuntimeException("Attempt to create new instance using invalid message type " + cmt);
			}
		}
	}
	

    private final byte msgTypeasByte;

    private MessageType(byte b) {
        this.msgTypeasByte = b;
    }

    public static MessageType fromByte(byte b) {
        for (MessageType mt : values()) {
            if (mt.msgTypeasByte == b) {
                return mt;
            }
        }
        return UNKNOWN;
    }

    public byte getByteValue() {
        return this.msgTypeasByte;
    }
    
}

