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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.math.BigDecimal;
import java.nio.ByteOrder;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;
import com.tesora.dve.sql.util.Pair;

public class MyUserVarLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyUserVarLogEvent.class);

	private static final int DIG_PER_DEC1 = 9;
	private static final int dig2bytes[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
	
	int variableNameLen;
	String variableName;
	byte nullByte;
	MyItemResultCode valueType;
	int valueCharSet;
	int valueLen;
	ByteBuf valueBytes;

	String variableValue;
	
	public enum MyItemResultCode {
		STRING_RESULT((byte) 0x00), 
		REAL_RESULT((byte) 0x01), 
		INT_RESULT((byte) 0x02),
		ROW_RESULT((byte) 0x03), 
		DECIMAL_RESULT((byte) 0x04);

		private final byte code;

		MyItemResultCode(byte b) {
			code = b;
		}

		public static MyItemResultCode fromByte(byte b) {
			for (MyItemResultCode mt : values()) {
				if (mt.code == b) {
					return mt;
				}
			}
			return null;
		}

		public byte getByteValue() {
			return code;
		}
	}

	public MyUserVarLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) throws PEException {
		variableNameLen = cb.readInt();
		variableName = MysqlAPIUtils.readBytesAsString(cb, variableNameLen, CharsetUtil.UTF_8);
		nullByte = cb.readByte();
		if (nullByte != 1) {
			variableValue = processVariableValue(cb);
		} else {
			variableValue = "NULL";
		}
	}

	String processVariableValue(ByteBuf cb) throws PEException {
		String value = StringUtils.EMPTY;

		valueType = MyItemResultCode.fromByte(cb.readByte());
		valueCharSet = cb.readInt();
		valueLen = cb.readInt();
		valueBytes = Unpooled.buffer(cb.readableBytes()).order(ByteOrder.LITTLE_ENDIAN);
		valueBytes.writeBytes(cb);

		
		switch(valueType) {
			case DECIMAL_RESULT:
				value = processDecimalValue(valueBytes, valueLen);
				break;
			case INT_RESULT:
				value = processIntValue(valueBytes, valueLen);
				break;
			case REAL_RESULT:
				value = Double.toString(valueBytes.readDouble());
				break;
			case STRING_RESULT:
				value = "'" + StringUtils.replace(MysqlAPIUtils.readBytesAsString(valueBytes, valueLen, CharsetUtil.UTF_8), "'", "''") + "'";
				break;
			case ROW_RESULT:
			default:
				throw new PEException("Unsupported variable type '" + valueType + "' for variable '" + variableName + "'");
		}
		return value;
	} 

	String processDecimalValue(ByteBuf cb, int valueLen) throws PEException {
		String value = StringUtils.EMPTY;
		
		byte precision = cb.readByte();
		byte scale = cb.readByte();
		
		Pair<Integer, Integer> intAndFracBinSize = getIntegerAndFractionBinSize(precision, scale);
		int binSize = intAndFracBinSize.getFirst() + intAndFracBinSize.getSecond();

		int readableBytes = cb.readableBytes();
		if ((intAndFracBinSize.getFirst() < 1 && intAndFracBinSize.getSecond() < 1) || readableBytes < binSize) {
			throw new PEException("Cannot decode binary decimal");
		}
		
		ByteBuf chunk = PooledByteBufAllocator.DEFAULT.heapBuffer(binSize);
		cb.readBytes(chunk);

		// 1st byte is special cause it determines the sign
		byte firstByte = chunk.getByte(0);
		int sign = (firstByte & 0x80) == 0x80 ? 1 : -1;
		// invert sign
		chunk.setByte(0, (firstByte ^ 0x80) );
		
		if (sign == -1) {
			// invert all the bytes
			for (int i = 0; i < binSize; i++) {
				chunk.setByte(i, ~chunk.getByte(i));
			}
		}

		BigDecimal integerPortion = decodeBinDecimal(chunk, intAndFracBinSize.getFirst(), true);
		BigDecimal fractionPortion = decodeBinDecimal(chunk, intAndFracBinSize.getSecond(), false);
		
		value = ((sign == -1) ? "-" : StringUtils.EMPTY) + integerPortion.toPlainString() + "." + fractionPortion.toPlainString();
		
		return value;
	}
	
	
	Pair<Integer, Integer> getIntegerAndFractionBinSize(int precision, int scale)
	{
		int intg = precision - scale;
		int intg0 = intg / DIG_PER_DEC1;
		int frac0 = scale / DIG_PER_DEC1;
		int intg0x = intg - intg0 * DIG_PER_DEC1;
		int frac0x = scale - frac0 * DIG_PER_DEC1;

		return new Pair<Integer, Integer>(intg0 * 4 + dig2bytes[intg0x], frac0 * 4 + dig2bytes[frac0x]);
	}

	BigDecimal decodeBinDecimal(ByteBuf cb, int bufferLen, boolean isIntegerPortion) throws PEException {
		BigDecimal decimalPortion = new BigDecimal(0);
		if (bufferLen > 0) {

			ByteBuf decimalPortionBuf = cb.readBytes(bufferLen);

			if (isIntegerPortion) {
				int initialBytes = bufferLen % 4;
				if (initialBytes > 0) {
					long intValue = readValue(decimalPortionBuf, initialBytes);
					decimalPortion = BigDecimal.valueOf(intValue);
				}
			}

			int decimalPortionLen = decimalPortionBuf.readableBytes();

			while (decimalPortionLen > 0) {
				int nextLen = (decimalPortionLen < 4) ? decimalPortionLen : 4;
				long intValue = readValue(decimalPortionBuf, nextLen);
				
				if (intValue > 0) {
					if (decimalPortion.longValue() == 0) {
						decimalPortion = decimalPortion.add(BigDecimal.valueOf(intValue));
					} else {
						int digits = (int)(Math.log10(intValue)+1);
						decimalPortion = decimalPortion.movePointRight(digits).add(BigDecimal.valueOf(intValue));
					}
				}
				
				decimalPortionLen = decimalPortionBuf.readableBytes();
			}
		}
		return decimalPortion;
	}

	long readValue(ByteBuf decimalPortionBuf, int valueLen) throws PEException {
		if (valueLen < 1 || valueLen > 4) throw new PEException("Cannot decode decimal buffer.  Invalid read length of " + valueLen);
		
		long value = 0;
		if (valueLen == 4) {
			value = decimalPortionBuf.readUnsignedInt();
		} else if (valueLen == 3) {
			value = decimalPortionBuf.readUnsignedMedium();
		} else if (valueLen == 2) {
			value = decimalPortionBuf.readUnsignedShort();
		} else if (valueLen == 1) {
			value = decimalPortionBuf.readUnsignedByte();
		}
		return value;
	}
	
	String processIntValue(ByteBuf cb, int valueLen) throws PEException {
		String value = StringUtils.EMPTY;
		
		switch(valueLen) {
			case 8:
				value = Long.toString(cb.readLong());
				break;
			case 7:
			case 6:
			case 5:
				throw new PEException("Cannot decode INT value of length '" + valueLen + "' for variable '" + variableName + "'");
			case 4:
				value = Long.toString(cb.readInt());
				break;
			case 3:
				value = Long.toString(cb.readMedium());
				break;
			case 2:
				value = Long.toString(cb.readShort());
				break;
			case 1:
				value = Byte.toString(cb.readByte());
				break;
		}
		return value;
	}
	
	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeInt(variableNameLen);
		cb.writeBytes(variableName.getBytes(CharsetUtil.UTF_8));
		cb.writeByte(nullByte);
		if (nullByte != 1) {
			cb.writeByte(valueType.getByteValue());
			cb.writeInt(valueCharSet);
			cb.writeInt(valueLen);
			cb.writeBytes(valueBytes);
		}
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) {
		if (logger.isDebugEnabled()) {
			logger.debug("** START UserVarLog Event **");
			logger.debug("Var Name: " + variableName);
			logger.debug("Var Value: " + variableValue);
			logger.debug("** END UserVarLog Event **");
		}
		plugin.getSessionVariableCache().setUserVariable(new Pair<String, String>(variableName, variableValue));
	}
}
