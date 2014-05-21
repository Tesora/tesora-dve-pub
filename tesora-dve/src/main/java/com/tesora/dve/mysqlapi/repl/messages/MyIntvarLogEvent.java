// OS_STATUS: public
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

import io.netty.buffer.ByteBuf;

import org.apache.log4j.Logger;

import com.google.common.primitives.UnsignedLong;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyIntvarLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyIntvarLogEvent.class);

	public enum MyIntvarEventVariableType {
		// MySql log events
		LAST_INSERT_ID_EVENT((byte) 0x01), 
		INSERT_ID_EVENT((byte) 0x02);

		private final byte typeasByte;

		MyIntvarEventVariableType(byte b) {
			typeasByte = b;
		}

		public static MyIntvarEventVariableType fromByte(byte b) {
			for (MyIntvarEventVariableType mt : values()) {
				if (mt.typeasByte == b) {
					return mt;
				}
			}
			return null;
		}

		public byte getByteValue() {
			return typeasByte;
		}
	}

	byte variableType;
	UnsignedLong variableValue;

	public MyIntvarLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		variableType = cb.readByte();
		variableValue = UnsignedLong.valueOf(cb.readLong());
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeByte(variableType);
		cb.writeLong(variableValue.longValue());
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) {
		boolean lastInsertIdEvent = (MyIntvarEventVariableType.fromByte(variableType) == MyIntvarEventVariableType.LAST_INSERT_ID_EVENT);
		if (logger.isDebugEnabled()) {
			logger.debug("** START Intvar Event **");
			logger.debug("Var Type: "
							+ ( lastInsertIdEvent ? "LAST_INSERT_ID_EVENT("
									+ MyIntvarEventVariableType.LAST_INSERT_ID_EVENT
									+ ")"
									: "INSERT_ID_EVENT("
											+ MyIntvarEventVariableType.INSERT_ID_EVENT
											+ ")"));
			logger.debug("Var Value: " + variableValue);
			logger.debug("** END Intvar Event **");
		}
		plugin.getSessionVariableCache().setIntVarValue(variableType, variableValue);
	}

	public byte getVariableType() {
		return variableType;
	}

	public void setVariableType(byte variableType) {
		this.variableType = variableType;
	}

	public UnsignedLong getVariableValue() {
		return variableValue;
	}

	public void setVariableValue(UnsignedLong variableValue) {
		this.variableValue = variableValue;
	}
}
