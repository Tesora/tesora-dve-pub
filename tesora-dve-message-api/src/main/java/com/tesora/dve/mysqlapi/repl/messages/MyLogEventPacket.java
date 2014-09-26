package com.tesora.dve.mysqlapi.repl.messages;

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

import com.tesora.dve.db.mysql.libmy.MyMarshallMessage;
import com.tesora.dve.db.mysql.libmy.MyUnmarshallMessage;
import com.tesora.dve.exceptions.PEException;

public abstract class MyLogEventPacket implements MyUnmarshallMessage,
		MyMarshallMessage, ReplicationVisitorEvent {

	static final Logger logger = Logger.getLogger(MyLogEventPacket.class);

	MyReplEventCommonHeader ch = null;
	boolean skipErrors = false;

	// used for testing
	boolean saveBinaryLogPosition = true;
	
	public MyLogEventPacket(MyReplEventCommonHeader ch) {
		this.ch = ch;
	}

	@Override
	public boolean isMessageTypeEncoded() {
		return false;
	}

	public MyReplEventCommonHeader getCommonHeader() {
		return ch;
	}

    //sub-classes must implement and dispatch to specific method.
    @Override
    public abstract void accept(ReplicationVisitorTarget visitorTarget) throws PEException;

    public boolean isSaveBinaryLogPosition() {
		return saveBinaryLogPosition;
	}

	public void setSaveBinaryLogPosition(boolean saveBinaryLogPosition) {
		this.saveBinaryLogPosition = saveBinaryLogPosition;
	}

	public String getSkipErrorMessage() {
		return "Replication Slave failed processing event type '" + 
				(MyLogEventType.fromByte(ch.type) != null ? MyLogEventType.fromByte(ch.type).name() : "type=" + ch.type) + 
				"' but slave_skip_errors is active. Replication processing will continue";
	}

	public boolean skipErrors() {
		return skipErrors;
	}
	
	public void setSkipErrors(boolean skip) {
		this.skipErrors = skip;
	}

	public static class MySqlLogEventFactory {

		public static MyLogEventPacket newInstance(MyLogEventType mlet, MyReplEventCommonHeader ch) throws PEException {
			MyLogEventPacket mlevp = null;

			switch (mlet) {
			case QUERY_EVENT:
				mlevp = (MyLogEventPacket) new MyQueryLogEvent(ch);
				break;

			case INTVAR_EVENT:
				mlevp = (MyLogEventPacket) new MyIntvarLogEvent(ch);
				break;

			case ROTATE_EVENT:
				mlevp = (MyLogEventPacket) new MyRotateLogEvent(ch);
				break;

			case FORMAT_DESCRIPTION_EVENT:
				mlevp = (MyLogEventPacket) new MyFormatDescriptionLogEvent(ch);
				break;

			case XID_EVENT:
				mlevp = (MyLogEventPacket) new MyXIdLogEvent(ch);
				break;

			case STOP_EVENT:
				mlevp = (MyStopLogEvent) new MyStopLogEvent(ch);
				break;
				
			case RAND_EVENT:
				mlevp = (MyRandLogEvent) new MyRandLogEvent(ch);
				break;
				
			case LOAD_EVENT:
				mlevp = (MyLoadLogEvent) new MyLoadLogEvent(ch);
				break;

			case CREATE_FILE_EVENT:
				mlevp = (MyCreateFileLogEvent) new MyCreateFileLogEvent(ch);
				break;

			case APPEND_BLOCK_EVENT:
				mlevp = (MyAppendBlockLogEvent) new MyAppendBlockLogEvent(ch);
				break;

			case EXEC_LOAD_EVENT:
				mlevp = (MyExecLoadLogEvent) new MyExecLoadLogEvent(ch);
				break;

			case DELETE_FILE_EVENT:
				mlevp = (MyDeleteFileLogEvent) new MyDeleteFileLogEvent(ch);
				break;

			case NEW_LOAD_EVENT:
				mlevp = (MyNewLoadLogEvent) new MyNewLoadLogEvent(ch);
				break;

			case USER_VAR_EVENT:
				mlevp = (MyUserVarLogEvent) new MyUserVarLogEvent(ch);
				break;

			case BEGIN_LOAD_QUERY_EVENT:
				mlevp = (MyBeginLoadLogEvent) new MyBeginLoadLogEvent(ch);
				break;

			case EXECUTE_LOAD_QUERY_EVENT:
				mlevp = (MyExecuteLoadLogEvent) new MyExecuteLoadLogEvent(ch);
				break;

			case TABLE_MAP_EVENT:
				mlevp = (MyTableMapLogEvent) new MyTableMapLogEvent(ch);
				break;

            case WRITE_ROWS_EVENT:
                mlevp = new MyLogWriteRowsPayload(ch);
                break;

            case UPDATE_ROWS_EVENT:_ROWS_EVENT:
                mlevp = new MyLogUpdateRowsPayload(ch);
                break;

            case DELETE_ROWS_EVENT:
                mlevp = new MyLogDeleteRowsPayload(ch);
                break;

			case SLAVE_EVENT:
				// This event is never written, so it cannot exist in a binary log file. It was meant for failsafe replication, which has never been implemented.
				throw new PEException(
						"No handler is implemented for log event type " + mlet);
				
			default:
                String message = "Attempt to create new instance using invalid log event type "
                        + mlet +" , returning raw framed payload";
                logger.warn(message);
                return new MyUnknownLogPayload(ch, mlet);
			}
			return mlevp;
		}
	}
}
