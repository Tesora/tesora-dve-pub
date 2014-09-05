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

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteOrder;
import java.util.List;

import com.tesora.dve.mysqlapi.repl.messages.MyComBinLogDumpRequest;
import com.tesora.dve.mysqlapi.repl.messages.MyComRegisterSlaveRequest;
import com.tesora.dve.mysqlapi.repl.messages.MyReplEvent;

public abstract class MyDecoder extends ByteToMessageDecoder {
	private boolean handshakeDone = false;
    int expectedSequence = 0;
    Packet mspPacket;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf inBigEndian, List<Object> out) throws Exception {
 		ByteBuf in = inBigEndian.order(ByteOrder.LITTLE_ENDIAN);

        if (mspPacket == null)
            mspPacket = new Packet(ctx.alloc(), expectedSequence, Packet.Modifier.HEAPCOPY_ON_READ, "replication");

        if (!mspPacket.decodeMore(ctx.alloc(),in)) //deals with framing and extended packets.
            return;

        byte lastSeq = mspPacket.getSequenceNumber();
        this.expectedSequence = mspPacket.getNextSequenceNumber();

        ByteBuf leHeader = mspPacket.unwrapHeader().order(ByteOrder.LITTLE_ENDIAN).retain();
        ByteBuf lePayload = mspPacket.unwrapPayload().order(ByteOrder.LITTLE_ENDIAN).retain();//retain a separate reference to the payload.
        mspPacket.release();
        mspPacket = null;

		MyMessage nativeMsg;

        nativeMsg = instantiateMessage(lePayload);

		// call the message specific unmarshall method the parse the message
		// contents
		((MyUnmarshallMessage) nativeMsg).unmarshallMessage(lePayload);

		// put the packet number into the message
		nativeMsg.setPacketNumber(lastSeq);
		out.add(nativeMsg);
	}

	abstract protected MyMessage instantiateMessage(ByteBuf frame);

	protected boolean isHandshakeDone() {
		return handshakeDone;
	}

	protected void setHandshakeDone(boolean handshakeDone) {
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
            String message = "Attempt to create new instance using invalid message type "
                    + mnmt +" , returning raw message";
//            logger.warn(message);
            mnm= new MyRawMessage();
		}
		return mnm;
	}

}
