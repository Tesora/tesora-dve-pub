package com.tesora.dve.mysqlapi;

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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteOrder;
import java.util.List;

import com.tesora.dve.db.mysql.libmy.MyEOFPktResponse;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyFieldPktFieldListResponse;
import com.tesora.dve.db.mysql.libmy.MyFieldPktResponse;
import com.tesora.dve.db.mysql.libmy.MyHandshakeV10;
import com.tesora.dve.db.mysql.libmy.MyMasterOKResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.db.mysql.libmy.MyUnmarshallMessage;
import com.tesora.dve.mysqlapi.repl.messages.MyComBinLogDumpRequest;
import com.tesora.dve.mysqlapi.repl.messages.MyComRegisterSlaveRequest;
import com.tesora.dve.mysqlapi.repl.messages.MyReplEvent;

public abstract class MyDecoder extends ByteToMessageDecoder {
	private boolean handshakeDone = false;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf inBigEndian, List<Object> out) throws Exception {
 		ByteBuf in = inBigEndian.order(ByteOrder.LITTLE_ENDIAN);

 		in.markReaderIndex();

		// check to see if we have the length field - a 3 byte integer
		if (in.readableBytes() < 3) {
			// The length field was not received yet - return null.
			// This method will be invoked again when more packets are
			// received and appended to the buffer.
			return ;
		}
		
		// The length field is in the buffer.

		// Read the length field. Add 1 to account for packetNumber field.
		int length = in.readUnsignedMedium() + 1;

		// Make sure there's enough bytes in the buffer.
		if (in.readableBytes() < length) {
			// The whole packet hasn't been received yet - return null.
			// This method will be invoked again when more packets are
			// received and appended to the buffer.

			// Reset to the marked position to read the length field again
			// next time.
			in.resetReaderIndex();

			return ;
		}

		// There's enough bytes in the buffer. Read it.
		ByteBuf frame = in.readBytes(length);

		MyMessage nativeMsg;
		byte packetNum = 0;
		// The first byte in the frame is the packet number
		packetNum = frame.readByte();

		nativeMsg = instantiateMessage(frame);

		// call the message specific unmarshall method the parse the message
		// contents
		((MyUnmarshallMessage) nativeMsg).unmarshallMessage(frame);

		// put the packet number into the message
		nativeMsg.setPacketNumber(packetNum);
		out.add(nativeMsg);
	}

	abstract MyMessage instantiateMessage(ByteBuf frame);

	boolean isHandshakeDone() {
		return handshakeDone;
	}

	void setHandshakeDone(boolean handshakeDone) {
		this.handshakeDone = handshakeDone;
	}

	protected MyMessage newResponseInstance(MyMessageType mnmt) {
		MyMessage mnm = null;

		switch (mnmt) {
		case FIELDPKT_RESPONSE:
			mnm = (MyMessage) new MyFieldPktResponse();
			break;
		case FIELDPKTFIELDLIST_RESPONSE:
			mnm = (MyMessage) new MyFieldPktFieldListResponse();
			break;
		case EOFPKT_RESPONSE:
			mnm = (MyMessage) new MyEOFPktResponse();
			break;
		case SERVER_GREETING_RESPONSE:
			mnm = (MyMessage) new MyHandshakeV10();
			break;
		case OK_RESPONSE:
			mnm = (MyMessage) new MyMasterOKResponse();
			break;
		case ERROR_RESPONSE:
			mnm = (MyMessage) new MyErrorResponse();
			break;

		case COM_REGISTER_SLAVE_REQUEST:
			mnm = (MyMessage) new MyComRegisterSlaveRequest();
			break;

		case COM_BINLOG_DUMP_REQUEST:
			mnm = (MyMessage) new MyComBinLogDumpRequest();
			break;

		case REPL_EVENT_RESPONSE:
			mnm = (MyMessage) new MyReplEvent();
			break;
		default:
			throw new RuntimeException(
					"Attempt to create new instance using invalid message type "
							+ mnmt);
		}
		return mnm;
	}

}
