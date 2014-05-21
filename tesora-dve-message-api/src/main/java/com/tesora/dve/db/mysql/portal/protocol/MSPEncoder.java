// OS_STATUS: public
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

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.exceptions.PESQLStateException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

@Sharable
public class MSPEncoder extends MessageToByteEncoder<MyMessage> {

	private static final Logger logger = Logger.getLogger(MSPEncoder.class);

	@Override
	protected void encode(ChannelHandlerContext ctx, MyMessage msg, ByteBuf out) throws Exception {
		ByteBuf leBuf = out.order(ByteOrder.LITTLE_ENDIAN);

        //TODO:It doesn't look like we ever instantiate MyResultSetResponse, and the dependency is problematic because of the usage of DBNative.  Need to verify this is OK.
//		if (msg instanceof MyResultSetResponse) {
//			MyResultSetResponse rsresp = (MyResultSetResponse) msg;
//			if (((MyResultSetResponse) msg).getMessageType() == MyMessageType.RESULTSET_RESPONSE)
//				encodeMessage(leBuf, rsresp);
//			Iterator<MyResponseMessage> iter = rsresp.getListIterator();
//			while (iter.hasNext()) {
//				encodeMessage(leBuf, (MyResponseMessage) iter.next());
//			}
//		} else {
        encodeMessage(leBuf, (MyMessage) msg);
//		}
	}

	static boolean PREFER_DIRECT = PlatformDependent.directBufferPreferred();

	// Taken from Netty so that we can control the size and location of the
	// buffer we allocate
	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		ByteBuf buf = null;
		try {
			if (acceptOutboundMessage(msg)) {
				@SuppressWarnings("unchecked")
				MyMessage cast = (MyMessage) msg;
				if (PREFER_DIRECT) {
					buf = ctx.alloc().ioBuffer(512);
				} else {
					buf = ctx.alloc().heapBuffer(512);
				}
				try {
					encode(ctx, cast, buf);
				} finally {
					ReferenceCountUtil.release(cast);
				}

				if (buf.isReadable()) {
					ctx.write(buf, promise);
				} else {
					buf.release();
					ctx.write(Unpooled.EMPTY_BUFFER, promise);
				}
				buf = null;
			} else {
				ctx.write(msg, promise);
			}
		} catch (EncoderException e) {
			throw e;
		} catch (Throwable e) {
			throw new EncoderException(e);
		} finally {
			if (buf != null) {
				buf.release();
			}
		}
	}

	private void encodeMessage(ByteBuf out, MyMessage message) throws PEException {

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

	private static final class InstanceHolder {
		private static final MSPEncoder INSTANCE = new MSPEncoder();
	}

	public static MSPEncoder getInstance() {
		return InstanceHolder.INSTANCE;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug(this.getClass().getSimpleName()
					+ " caught exception - exception ignored (possibly due to client disconnect)", cause);
		if (!IOException.class.equals(cause.getClass()) || !"Connection reset by peer".equals(cause.getMessage()))
			super.exceptionCaught(ctx, cause);
	}
}
