// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.nio.ByteOrder;
import java.util.List;

import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public class MSPProtocolDecoder extends ReplayingDecoder<MSPProtocolDecoder.MyDecoderState> {

	private static final int EXTPKT_INDICATOR = 0xFFFFFF;

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
        for(MSPMessage m : mspMessages)
            messageMap[m.getMysqlMessageType()] = m;
    }

	private int length;
	private byte sequenceId;
	private byte messageType;
	private ByteBuf extendedPacket;

    private MSPMessage[] messageExecutor;


	public MSPProtocolDecoder() throws PEException {
        this(MyDecoderState.READ_CLIENT_AUTH);
	}

    public MSPProtocolDecoder(MyDecoderState initialState) throws PEException {
        super(initialState);
        this.messageExecutor = messageMap;
    }

	@Override
	protected void decode(final ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
//		String origThreadName = Thread.currentThread().getName();
//		Thread.currentThread().setName("Netty worker for " + ssCon.getName());
//		logger.debug("got packet " + in);
		
		try {
			ByteBuf inBuf = in.order(ByteOrder.LITTLE_ENDIAN);

			switch (this.state()) {
            case READ_SERVER_GREETING:
            case READ_CLIENT_AUTH:
            {
                int length = inBuf.readUnsignedMedium();
                byte sequenceId = inBuf.readByte();
                ByteBuf authPayload = inBuf.readSlice(length).order(ByteOrder.LITTLE_ENDIAN);
                authPayload.retain();//required, since we won't be holding a reference to the original buffer.
                MSPMessage authMessage;
                if (this.state() == MyDecoderState.READ_CLIENT_AUTH)
                    authMessage = new MSPAuthenticateV10MessageMessage(sequenceId,authPayload);
                else if (this.state() == MyDecoderState.READ_SERVER_GREETING){
                    authMessage = new MSPServerGreetingRequestMessage(sequenceId,authPayload);
                } else {
                    throw new PECodingException("Unexpected state in packet decoding, "+this.state());
                }
                out.add(authMessage);
                checkpoint(MyDecoderState.READ_HEADER);
                break;
            }
			case READ_HEADER:
				if (inBuf.readableBytes() >= MESSAGE_HEADER_LENGTH) {
					length = inBuf.readUnsignedMedium();
					sequenceId = inBuf.readByte();

					if (length == EXTPKT_INDICATOR) {
//						logger.debug("reading extended packet");
						checkpoint(MyDecoderState.READ_FIRST_EXTENDEDPACKET);
					} else {
						checkpoint(MyDecoderState.READ_CONTENT);
					}
				}
				break;

			case READ_CONTENT:
				if (inBuf.readableBytes() >= length) {
					messageType = inBuf.readByte();
//					final ByteBuf msgBuf = inBuf.slice(inBuf.readerIndex(), length-1).order(ByteOrder.LITTLE_ENDIAN);
					final ByteBuf msgBuf = inBuf.readBytes(length-1).order(ByteOrder.LITTLE_ENDIAN);
//					inBuf.skipBytes(length-1);
//					final byte theMessageType = messageType;
//					final byte theSequenceId = sequenceId;
                    MSPMessage mspMessage;
                    try{
                        mspMessage = messageExecutor[messageType].newPrototype(sequenceId,msgBuf);
                    } catch (Exception e){
                        mspMessage = new MSPUnknown(messageType,sequenceId,msgBuf);
                    }
                    out.add(mspMessage);
					reset();
				}
				break;

			case READ_FIRST_EXTENDEDPACKET:
				if ( extendedPacket == null )
					extendedPacket = Unpooled.buffer(length+1).order(ByteOrder.LITTLE_ENDIAN);
				messageType = inBuf.readByte();
//				logger.debug("reading first extended packet of type " + messageType);
				extendedPacket.writeBytes(MysqlAPIUtils.readBytes(inBuf, length-1));
				checkpoint(MyDecoderState.READ_NEXT_EXTENDEDPACKET);
				break;

			case READ_NEXT_EXTENDEDPACKET:
				length = inBuf.readUnsignedMedium();

				final byte extPacketNum = inBuf.readByte(); // need to store the last packet num - the OK response needs to use it.
//				logger.debug("Reading subsequent extended packet "+extPacketNum+": " + extendedPacket);
				extendedPacket.writeBytes(MysqlAPIUtils.readBytes(inBuf, length)); // append the payload to the frame

				if (length == EXTPKT_INDICATOR) {
//					logger.debug("Waiting for next extended packet");
					checkpoint(MyDecoderState.READ_NEXT_EXTENDEDPACKET);
				} else {
//					logger.debug("Extended packet complete - processing");
					final ByteBuf packetToExecute = extendedPacket;
					final byte messageType = this.messageType;
					extendedPacket = null;
                    MSPMessage mspMessage;
                    try{
                        mspMessage = messageExecutor[messageType].newPrototype(sequenceId,packetToExecute);
                    } catch (Exception e){
                        mspMessage = new MSPUnknown(messageType,sequenceId,packetToExecute);
                    }
                    out.add(mspMessage);
					reset();
				}
				break;
			}
		} finally {
//			Thread.currentThread().setName(origThreadName);
			PEThreadContext.clear();
		}
	}

	private void reset() {
		checkpoint(MyDecoderState.READ_HEADER);
		length = 0;
		messageType = -1;
		if ( extendedPacket != null )
			extendedPacket.release();
	}

	public enum MyDecoderState {
		READ_SERVER_GREETING, READ_CLIENT_AUTH, READ_HEADER, READ_CONTENT, READ_FIRST_EXTENDEDPACKET, READ_NEXT_EXTENDEDPACKET;
	}
}
