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
import io.netty.handler.codec.ReplayingDecoder;

import java.nio.ByteOrder;
import java.util.List;

import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import org.slf4j.LoggerFactory;

public class MSPProtocolDecoder extends ByteToMessageDecoder {

	private static final int MESSAGE_HEADER_LENGTH = 4;

	private final static MSPMessage mspMessages[] = {
			new MSPComQueryRequestMessage(),
			new MSPComFieldListRequestMessage(),
			new MSPComQuitRequestMessage(),
			new MSPComSetOptionRequestMessage(),
			new MSPComPingRequestMessage(),
			new MSPComInitDBRequestMessage(),
			new MSPComPrepareStmtRequestMessage(),
			new MSPComStmtExecuteRequestMessage(), //TODO:when we receive ok prepare responses, need to update the execute prototype, since the message is context sensitive.
			new MSPComStmtCloseRequestMessage(),
			new MSPComProcessInfoRequestMessage(),
			new MSPComStatisticsRequestMessage()
	};

	private final static MSPMessage[] messageMap = new MSPMessage[256];
	static {
		for (final MSPMessage m : mspMessages) {
			messageMap[m.getMysqlMessageType()] = m;
		}
	}

	private Packet packet;

	private final MSPMessage[] messageExecutor;

    private MyDecoderState currentState;

	public MSPProtocolDecoder() throws PEException {
		this(MyDecoderState.READ_CLIENT_AUTH);
	}

	public MSPProtocolDecoder(MyDecoderState initialState) throws PEException {
		super();
		this.messageExecutor = messageMap;
        this.currentState = initialState;
	}

	@Override
	protected void decode(final ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		//		String origThreadName = Thread.currentThread().getName();
		//		Thread.currentThread().setName("Netty worker for " + ssCon.getName());
		//		logger.debug("got packet " + in);

		try {
            final ByteBuf inBuf = in.order(ByteOrder.LITTLE_ENDIAN);

            if (inBuf.readableBytes() < MESSAGE_HEADER_LENGTH)
                return;

            inBuf.markReaderIndex();
            final int payloadLength = inBuf.readUnsignedMedium();
            final byte sequenceId = inBuf.readByte();

            if (inBuf.readableBytes() < payloadLength){
                inBuf.resetReaderIndex();
                return;
            }

            //OK, now we know we have an entire frame available for read.

			final MyDecoderState state = currentState;
			switch (state) {
			case READ_SERVER_GREETING:
			case READ_CLIENT_AUTH: {

				final ByteBuf authPayload = inBuf.readSlice(payloadLength).order(ByteOrder.LITTLE_ENDIAN);
				authPayload.retain();//required, since we won't be holding a reference to the original buffer.
				MSPMessage authMessage;
				if (state == MyDecoderState.READ_CLIENT_AUTH) {
					authMessage = new MSPAuthenticateV10MessageMessage(sequenceId, authPayload);
				} else if (state == MyDecoderState.READ_SERVER_GREETING) {
					authMessage = new MSPServerGreetingRequestMessage(sequenceId, authPayload);
				} else {
					throw new PECodingException("Unexpected state in packet decoding, " + state);
				}
				out.add(authMessage);
				checkpoint(MyDecoderState.READ_PACKET);
				break;
			}

			case READ_PACKET:


                final byte messageType = inBuf.readByte();
                this.packet = Packet.buildPacket(payloadLength);//TODO: this is weird, we are passing length, but already pulled off one octect. -sgossard
                this.packet.setSequenceId(sequenceId);
                this.packet.setMessageType(messageType);
                this.packet.writePacketPayload(inBuf, payloadLength - 1, sequenceId);

                if (this.packet.isExtended()) {
                    checkpoint(MyDecoderState.READ_NEXT_EXTENDEDPACKET);
                } else {
                    emitMessageAndResetDecoder(out);
                }
				break;

			case READ_NEXT_EXTENDEDPACKET:
				this.packet.writePacketPayload(inBuf, payloadLength, sequenceId); // append the payload to the frame

				if (payloadLength < MysqlNativeConstants.MAX_PAYLOAD_SIZE) {
					emitMessageAndResetDecoder(out);
				} else {
					checkpoint(MyDecoderState.READ_NEXT_EXTENDEDPACKET);
				}
				break;
			}
		} catch (Exception e){
            LoggerFactory.getLogger(MSPProtocolDecoder.class).warn("SMG: problem decoding frame",e);
        }finally {
			//			Thread.currentThread().setName(origThreadName);
			PEThreadContext.clear();
		}
	}

	private void emitMessageAndResetDecoder(final List<Object> out) {
		final byte packetSequenceId = this.packet.getSequenceId();
		final byte packetMessageType = this.packet.getMessageType();
		final ByteBuf packetPayload = this.packet.getPayload();

		this.packet = null; // Do not release the payload buffer here.
		out.add(buildMessage(packetPayload, packetMessageType, packetSequenceId));
		reset();
	}

	private MSPMessage buildMessage(final ByteBuf frame, final byte messageType, final byte sequenceId) {
		try {
			return this.messageExecutor[messageType].newPrototype(sequenceId, frame);
		} catch (final Exception e) {
			return new MSPUnknown(messageType, sequenceId, frame);
		}
	}

    protected void checkpoint(MyDecoderState state) {
        this.currentState = state;
    }

	private void reset() {
		this.checkpoint(MyDecoderState.READ_PACKET);
		if (packet != null) {
			packet.getPayload().release();
			packet = null;
		}
	}

	public enum MyDecoderState {
		READ_SERVER_GREETING, READ_CLIENT_AUTH, READ_PACKET, READ_NEXT_EXTENDEDPACKET;
	}
}
