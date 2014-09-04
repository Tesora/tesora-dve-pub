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
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;

public class MyFormatDescriptionLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger.getLogger(MyFormatDescriptionLogEvent.class);

	private static final int ST_SERVER_VER_LEN = 50;

	public enum MyBinLogVerType {
		MySQL_3_23((byte) 0x01), MySQL_4_0_2_to_4_1((byte) 0x03), MySQL_5_0((byte) 0x04);

		private final byte typeasByte;

		MyBinLogVerType(byte b) {
			typeasByte = b;
		}

		public static MyBinLogVerType fromByte(byte b) {
			for (MyBinLogVerType mt : values()) {
				if (mt.typeasByte == b) {
					return mt;
				}
			}
			return null;
		}
	}

	short binaryLogVersion;
	String serverVersion;
	long createTime;
    short eventTypeLength;
	Map<MyLogEventType, Byte> eventTypeValues = new HashMap<MyLogEventType, Byte>();

	public MyFormatDescriptionLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

    @Override
    public void accept(ReplicationVisitorTarget visitorTarget) throws PEException { visitorTarget.visit((MyFormatDescriptionLogEvent)this);}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		binaryLogVersion = cb.readShort();
		serverVersion = MysqlAPIUtils.readBytesAsString(cb, ST_SERVER_VER_LEN, CharsetUtil.UTF_8);
		createTime = cb.readUnsignedInt();
		eventTypeLength = cb.readUnsignedByte();
        int numberOfEntries = cb.readableBytes() - 1;//string is null terminated.

		switch (MyBinLogVerType.fromByte((byte) binaryLogVersion)) {
		
		case MySQL_5_0:
			for (int i = 1; i <= numberOfEntries; i++) {
				eventTypeValues.put(MyLogEventType.fromByte((byte) i), cb.readByte());
			}
            cb.skipBytes(1);//throw out EOF
			break;

		default:
			// TODO throw????
			logger.error("Cannot process binary log messages that are not for MySQL 5.0");
		}
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeShort(binaryLogVersion);
		cb.writeBytes(serverVersion.getBytes(CharsetUtil.UTF_8));
		cb.writeInt((int) createTime);
        cb.writeByte(0xFF & eventTypeLength);
		switch (MyBinLogVerType.fromByte((byte) binaryLogVersion)) {

		case MySQL_5_0:
			for (int i = 1; i <= eventTypeValues.size(); i++) {
				cb.writeByte(eventTypeValues.get(MyLogEventType.fromByte((byte) i)));
			}
            cb.writeZero(1);
			break;

		default:
			break;
		}
	}

	public short getBinaryLogVersion() {
		return binaryLogVersion;
	}

	public void setBinaryLogVersion(short binaryLogVersion) {
		this.binaryLogVersion = binaryLogVersion;
	}

	public Map<MyLogEventType, Byte> getEventTypeValues() {
		return eventTypeValues;
	}

	public void setEventTypeValues(Map<MyLogEventType, Byte> eventTypeValues) {
		this.eventTypeValues = eventTypeValues;
	}
}
