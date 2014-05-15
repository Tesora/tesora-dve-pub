// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyUserVarLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyUserVarLogEvent.class);

	ByteBuf variableData; 
//	int nameLen;
//	String variableName;
//	byte variableValue;
//	MyItemResultCode variableType = null;
//	Integer charSetNum = null;
//	Integer variableSize = null;
//	String variableValueString = null;
//	Double variableValueReal = null; 
//	Long variableValueInteger = null;
//	String variableValueDecimal = null;
//	
//	public enum MyItemResultCode {
//		STRING_RESULT((byte) 0x00), 
//		REAL_RESULT((byte) 0x01), 
//		INT_RESULT((byte) 0x02),
//		ROW_RESULT((byte) 0x03), 
//		DECIMAL_RESULT((byte) 0x04);
//
//		private final byte code;
//
//		MyItemResultCode(byte b) {
//			code = b;
//		}
//
//		public static MyItemResultCode fromByte(byte b) {
//			for (MyItemResultCode mt : values()) {
//				if (mt.code == b) {
//					return mt;
//				}
//			}
//			return null;
//		}
//
//		public byte getByteValue() {
//			return code;
//		}
//	}

	public MyUserVarLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
//		nameLen = cb.readInt();
//		variableName = cb.readBytes(nameLen).toString(CharsetUtil.UTF_8);
//		variableValue = cb.readByte();
//		processVariableValue(cb, variableValue);
		// TODO: need to parse out the variable part of the data
		variableData = Unpooled.buffer(cb.readableBytes());
		variableData.writeBytes(cb);
	}

	/**
	 * INCOMPLETE IMPLEMENTATION
	 * @param cb
	 * @param varValue
	 */
//	void processVariableValue(ByteBuf cb, byte varValue) {
//		if (varValue != 0) {
//			// Non-zero if the variable value is the SQL NULL value
//		} else {
//			// If this byte is 0, the following parts exist in the event.
//			// 1 byte. The user variable type. The value corresponds to elements 
//			// of enum Item_result defined in include/mysql_com.h 
//			// (STRING_RESULT=0, REAL_RESULT=1, INT_RESULT=2, ROW_RESULT=3, DECIMAL_RESULT=4).
//			variableType = MyItemResultCode.fromByte(cb.readByte());
//			
//			// 4 bytes. The number of the character set for the user variable 
//			// (needed for a string variable). The character set number is 
//			// really a collation number that indicates a character set/collation pair.
//			charSetNum = cb.readInt();
//			
//			// 4 bytes. The size of the user variable value 
//			// (corresponds to member val_len of class Item_string).
//			variableSize = cb.readInt();
//			
//			// Variable-sized. For a string variable, this is the string. 
//			// For a float or integer variable, this is its value in 8 bytes. 
//			// For a decimal this value is a packed value - 1 byte for the 
//			// precision, 1 byte for the scale, and $size - 2 bytes for the 
//			// actual value. See the decimal2bin function in strings/decimal.c 
//			// for the format of this packed value. 
//			switch(variableType) {
//			case STRING_RESULT:
//				variableValueString = cb.readBytes(variableSize).toString();
//				break;
//			case REAL_RESULT:
//				variableValueReal = Double.valueOf(cb.readDouble());
//				break;
//			case INT_RESULT:
//				variableValueInteger = Long.valueOf(cb.readLong());
//				break;
//			case DECIMAL_RESULT:
////				byte precision = cb.readByte();
////				byte scale = cb.readByte();
////				byte sign = cb.readByte();
////				ByteBuf decBuf = cb.readBytes(variableSize-3);
////				int integerPortionLen = precision-scale;
////
////				String decimalValue = "";
////				// read integer part as 4 byte chunks
////				while (true) {
////					int nextLen = (integerPortionLen < 4) ? integerPortionLen : 4;
////					long intValue;
////					if (nextLen == 4) {
////						intValue = decBuf.readUnsignedInt();
////					} else if (nextLen == 3) {
////						intValue = decBuf.readMedium();
////					} else if (nextLen == 3) {
////						intValue = decBuf.readShort();
////					} else if (nextLen == 1) {
////						intValue = decBuf.readByte();
////					}
////					decimalValue += Long.toString(intValue);
////				}
////				
////				// read fraction 
//				break;
//			case ROW_RESULT:
//			default:
//				// unsupported, throw??
//			}
//		}
//	} 
	
	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeBytes(variableData);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) {
//		logger.debug("** START UserVarLog Event **");
//		logger.debug("** END UserVarLog Event **");
		logger.warn("Message is parsed but no handler is implemented for log event type: USER_VAR_EVENT");
	}
}
