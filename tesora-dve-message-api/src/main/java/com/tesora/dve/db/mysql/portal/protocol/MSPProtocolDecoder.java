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

import com.tesora.dve.db.mysql.libmy.MyMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import org.slf4j.LoggerFactory;

public class MSPProtocolDecoder extends ChannelDuplexHandler {

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

    //TODO: tracking this as a single entry assumes we won't get pipelined requests from the client. -sgossard
    byte nextSequence = 1;

    CachedAppendBuffer cachedAppendBuffer = new CachedAppendBuffer();
    private final ByteToMessageDecoder decoder = new ByteToMessageDecoder() {
        @Override
        public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            MSPProtocolDecoder.this.decode(ctx, in, out);
        }

        @Override
        protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            MSPProtocolDecoder.this.decode(ctx, in, out);
        }
    };

	public MSPProtocolDecoder(MyDecoderState initialState) throws PEException {
		super();
		this.messageExecutor = messageMap;
        this.currentState = initialState;
	}

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        decoder.channelInactive(ctx);
        cachedAppendBuffer.releaseSlab();
        if (mspPacket != null){
            mspPacket.release();
            mspPacket = null;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        decoder.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof MyMessage) {
            MyMessage message = (MyMessage)msg;
            ByteBuf appendableView = cachedAppendBuffer.startAppend(ctx);
            nextSequence = (byte)(0xFF & Packet.encodeFullMessage(nextSequence, message, appendableView));
            ByteBuf writableSlice = cachedAppendBuffer.sliceWritableData();
            ctx.write(writableSlice, promise);
        } else {
            //forward packet along.
            ctx.write(msg, promise);
        }
    }

	protected void decode(final ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
	    try {

            if (mspPacket == null) {
                //assumes next full inbound request will have a starting sequence (true except for auth handshake)
                //also assumes we only get one request at a time (synchronous), which is typical of most mysql clients
                int seq = firstPacket ? 1 : 0;
                mspPacket = new Packet(ctx.alloc(), seq, Packet.Modifier.HEAPCOPY_ON_READ,"frontend");
            }

            //deals with the handshake packet
            firstPacket = false;

            if (!mspPacket.decodeMore(in))
                return;


            int sequenceId = mspPacket.getSequenceNumber();
            this.nextSequence = (byte)(0xFF & mspPacket.getNextSequenceNumber()); //save off the sequence for our outbound response.

            ByteBuf payload = mspPacket.unwrapPayload().retain();//retain a separate reference to the payload.
            mspPacket.release();
            mspPacket = null;

			final MyDecoderState state = currentState;
			switch (state) {
			case READ_SERVER_GREETING:
			case READ_CLIENT_AUTH: {
				MSPMessage authMessage;
				if (state == MyDecoderState.READ_CLIENT_AUTH) {
					authMessage = MSPAuthenticateV10MessageMessage.newMessage(payload);
				} else if (state == MyDecoderState.READ_SERVER_GREETING) {
					authMessage = new MSPServerGreetingRequestMessage(payload);
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
			return this.messageExecutor[messageType].newPrototype(payload);
		} catch (final Exception e) {
			return new MSPUnknown(messageType, payload);
		}
	}

    public enum MyDecoderState {
		READ_SERVER_GREETING, READ_CLIENT_AUTH, READ_PACKET
	}
}
