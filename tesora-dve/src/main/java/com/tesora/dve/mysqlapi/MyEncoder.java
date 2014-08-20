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

import com.tesora.dve.db.mysql.portal.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteOrder;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.exceptions.PEException;


public class MyEncoder extends MessageToByteEncoder<MyMessage> {
	private static final Logger logger = Logger.getLogger(MyEncoder.class);

	public MyEncoder() {
	}

	public ByteBuf encodeMessage(MyMessage message) throws PEException {

		if (logger.isDebugEnabled())
			logger.debug("Encoding message of type " + message.getMessageType().toString());

		ByteBuf buffer = Unpooled.buffer(2048).order(ByteOrder.LITTLE_ENDIAN);

        Packet.encodeFullMessage(message.getSeq(), message, buffer);

        return buffer;
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, MyMessage msg, ByteBuf out) throws Exception {
		out.writeBytes( encodeMessage(msg) );
	}

}
