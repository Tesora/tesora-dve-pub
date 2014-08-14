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
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.exceptions.PESQLStateException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteOrder;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public class MSPEncoder extends MessageToByteEncoder<MysqlMessage> {
    private static final Logger logger = Logger.getLogger(MSPEncoder.class);

    CachedAppendBuffer appender = new CachedAppendBuffer();

    @Override
    protected void encode(ChannelHandlerContext ctx, MysqlMessage msg, ByteBuf out) throws Exception {
        ByteBuf leBuf = out.order(ByteOrder.LITTLE_ENDIAN);
        logMessageIfNeeded(msg);
        try{
            msg.writeTo(leBuf);
        } finally {
            ReferenceCountUtil.release(msg);  //since we are forwarding a bytebuf not the original message, we need to release the message.
        }
    }


    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        try {
            super.disconnect(ctx, promise);
        } finally {
            appender.releaseSlab();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        try {
            super.handlerRemoved(ctx);
        } finally {
            appender.releaseSlab();
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

                ByteBuf appendableView = appender.startAppend(ctx);
                encode(ctx,cast,appendableView);
                ByteBuf writableSlice = appender.sliceWritableData();
				ctx.write(writableSlice, promise);
			} else {
				ctx.write(msg, promise);
			}
		} catch (EncoderException e) {
            throw e;
		} catch (Throwable e) {
			throw new EncoderException(e);
		}
	}

    public static void logMessageIfNeeded(MysqlMessage message) {
        if (message instanceof MyResponseMessage) {

            MyResponseMessage response = (MyResponseMessage) message;
            if (response.isErrorResponse()) {
                //we are sending an error response to the client, so we'll log it.
                Exception baseException = (response.hasException() ? response.getException() : null);
                boolean rootCauseIsMysqlErrorPacket = false;
                if (baseException instanceof PEException) {
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
    }

    public static MSPEncoder getInstance() {
		return new MSPEncoder();
	}

}
