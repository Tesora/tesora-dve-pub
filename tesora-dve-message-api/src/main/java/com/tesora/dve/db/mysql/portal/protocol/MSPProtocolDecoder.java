package com.tesora.dve.db.mysql.portal.protocol;

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

import java.util.List;

import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import org.slf4j.LoggerFactory;

public class MSPProtocolDecoder extends ByteToMessageDecoder {

	private final static MSPMessage mspMessages[] = {
			MSPComQueryRequestMessage.PROTOTYPE,
			MSPComFieldListRequestMessage.PROTOTYPE,
			MSPComQuitRequestMessage.PROTOTYPE,
			MSPComSetOptionRequestMessage.PROTOTYPE,
			MSPComPingRequestMessage.PROTOTYPE,
			MSPComInitDBRequestMessage.PROTOTYPE,
			MSPComPrepareStmtRequestMessage.PROTOTYPE,
			MSPComStmtExecuteRequestMessage.PROTOTYPE,
			MSPComStmtCloseRequestMessage.PROTOTYPE,
			MSPComProcessInfoRequestMessage.PROTOTYPE,
			MSPComStatisticsRequestMessage.PROTOTYPE
	};

	private final static MSPMessage[] messageMap = new MSPMessage[256];
	static {
		for (final MSPMessage m : mspMessages) {
			messageMap[m.getMysqlMessageType()] = m;
		}
	}

	private final MSPMessage[] messageExecutor;

    private MyDecoderState currentState;

    private Packet mspPacket;
    public boolean firstPacket = true;

	public MSPProtocolDecoder(MyDecoderState initialState) throws PEException {
		super();
		this.messageExecutor = messageMap;
        this.currentState = initialState;
	}


    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try{
            super.decodeLast(ctx, in, out);
        } finally {
            if (mspPacket != null){
                mspPacket.release();
                mspPacket = null;
            }
        }
    }

    @Override
	protected void decode(final ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
	    try {
            if (mspPacket == null)
                mspPacket = new Packet(ctx.alloc(), firstPacket ? 1 : 0, Packet.Modifier.HEAPCOPY_ON_READ,"frontend");

            //deals with the handshake packet
            firstPacket = false;

            if (!mspPacket.decodeMore(in))
                return;

            int sequenceId = mspPacket.getSequenceNumber();
            ByteBuf payload = mspPacket.unwrapPayload().retain();//retain a separate reference to the payload.
            mspPacket.release();
            mspPacket = null;

			final MyDecoderState state = currentState;
			switch (state) {
			case READ_SERVER_GREETING:
			case READ_CLIENT_AUTH: {
				MSPMessage authMessage;
				if (state == MyDecoderState.READ_CLIENT_AUTH) {
					authMessage = MSPAuthenticateV10MessageMessage.newMessage((byte)sequenceId, payload);
				} else if (state == MyDecoderState.READ_SERVER_GREETING) {
					authMessage = new MSPServerGreetingRequestMessage((byte)sequenceId, payload);
				} else {
					throw new PECodingException("Unexpected state in packet decoding, " + state);
				}
				out.add(authMessage);
                this.currentState = MyDecoderState.READ_PACKET;
                break;
			}
			case READ_PACKET:
                out.add(buildMessage(payload, (byte)sequenceId));
				break;
			}
		} catch (Exception e){
            LoggerFactory.getLogger(MSPProtocolDecoder.class).warn("problem decoding frame",e);
        }finally {
			//			Thread.currentThread().setName(origThreadName);
			PEThreadContext.clear();
		}
	}

	private MSPMessage buildMessage(final ByteBuf payload, final byte sequenceId) {
        final byte messageType = payload.getByte(0); //peek at the first byte in the payload to determine message type.
		try {
			return this.messageExecutor[messageType].newPrototype(sequenceId, payload);
		} catch (final Exception e) {
			return new MSPUnknown(messageType, sequenceId, payload);
		}
	}

    public enum MyDecoderState {
		READ_SERVER_GREETING, READ_CLIENT_AUTH, READ_PACKET
	}
}
