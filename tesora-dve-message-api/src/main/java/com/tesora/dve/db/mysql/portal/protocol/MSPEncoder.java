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


import com.tesora.dve.db.mysql.MysqlMessage;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.exceptions.PESQLStateException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public class MSPEncoder extends MessageToByteEncoder<MysqlMessage> {
    private static final Logger logger = Logger.getLogger(MSPEncoder.class);
    public static final int SLAB_SIZE = 1024 * 8;//start with a 8K slab for transcoding outbound writes.

    static boolean PREFER_DIRECT = PlatformDependent.directBufferPreferred();

    ByteBuf cachedSlab;

    @Override
    protected void encode(ChannelHandlerContext ctx, MysqlMessage msg, ByteBuf out) throws Exception {
        ByteBuf leBuf = out.order(ByteOrder.LITTLE_ENDIAN);

        if (msg instanceof MyMessage)
            encodeMyMessage(leBuf, (MyMessage)msg);
        else
            encodeMSPMessage((MSPMessage)msg,leBuf);
    }

    protected void encodeMSPMessage(MSPMessage msg, ByteBuf littleEnd) throws Exception {
        msg.writeTo(littleEnd);
    }



    private void allocateSlabIfNeeded(ChannelHandlerContext ctx) {
        if (cachedSlab != null) //we already have a slab.
            return;

        if (PREFER_DIRECT) {
            cachedSlab = ctx.alloc().ioBuffer(SLAB_SIZE);
        } else {
            cachedSlab = ctx.alloc().heapBuffer(SLAB_SIZE);
        }

        cachedSlab = cachedSlab.order(ByteOrder.LITTLE_ENDIAN);
    }

    private void releaseSlab() {
        ReferenceCountUtil.release(cachedSlab);
        cachedSlab = null;
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        try {
            super.disconnect(ctx,promise);
        } finally {
            releaseSlab();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        try {
            super.handlerRemoved(ctx);
        } finally {
            releaseSlab();
        }
    }

    // Taken from Netty so that we can control the size and location of the
	// buffer we allocate
	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		try {
			if (acceptOutboundMessage(msg)) {
				@SuppressWarnings("unchecked")
                MysqlMessage cast = (MysqlMessage) msg;

                allocateSlabIfNeeded(ctx);

                //if we don't have any outbound slices holding references, reset to full slab.
				if (cachedSlab.refCnt() == 1){
                    cachedSlab.clear();
                }

                int startingWriterIndex = cachedSlab.writerIndex();
                encode(ctx,cast,cachedSlab);
                int writtenLength = cachedSlab.writerIndex() - startingWriterIndex;

				if (writtenLength > 0) {
                    ByteBuf sliceWritten = cachedSlab.slice(startingWriterIndex, writtenLength);
                    sliceWritten.retain(); //slices aren't retained by default, increments the ref count on the parent buffer.
					ctx.write(sliceWritten, promise);
                    //TODO: we may want to consider recycling large slabs, to avoid holding an unusually big bufferindefinitely. -sgossard
				} else {
					ctx.write(Unpooled.EMPTY_BUFFER, promise);
				}
			} else {
				ctx.write(msg, promise);
			}
		} catch (EncoderException e) {
            throw e;
		} catch (Throwable e) {
			throw new EncoderException(e);
		}
	}

	private void encodeMyMessage(ByteBuf out, MyMessage message) throws PEException {

		if (message instanceof MyResponseMessage) {

            MyResponseMessage response = (MyResponseMessage) message;
            if (response.isErrorResponse()){
                //we are sending an error response to the client, so we'll log it.
                Exception baseException = (response.hasException() ? response.getException() : null);
                boolean rootCauseIsMysqlErrorPacket = false;
                if (baseException instanceof PEException){
                    Exception rootCause = ((PEException) baseException).rootCause();
                    rootCauseIsMysqlErrorPacket = (rootCause instanceof PESQLStateException);
                }

                //reduce log chattiness and only show stack trace if root cause wasn't a mysql error packet response.
                if (rootCauseIsMysqlErrorPacket || baseException == null)
                    logger.warn("Encoding " + message);
                else
                    logger.warn("Encoding " + message, baseException);
            }

		} else if (logger.isDebugEnabled()) {
			logger.debug("Encoding " + message);
		}

		int msgSizeIndex = out.writerIndex();
		out.writeMedium(0);
		out.writeByte(message.getPacketNumber());
		int msgStartIndex = out.writerIndex();

		// check if the message type is to be encoded
		if (message.isMessageTypeEncoded())
			out.writeByte(message.getMessageType().getByteValue());

		// ask the message object to write the message payload
		message.marshallMessage(out);

		// Go back and write the payload size into the header
		int payloadSize = out.writerIndex() - msgStartIndex;
		out.setMedium(msgSizeIndex, payloadSize);
		out.setByte(msgSizeIndex + 3, message.getPacketNumber());
	}

	public static MSPEncoder getInstance() {
		return new MSPEncoder();
	}

}
