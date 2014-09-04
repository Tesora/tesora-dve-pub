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
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyMessageType;

import java.util.List;

public class MyClientSideAsyncDecoder extends MyDecoder {
	private static final Logger logger = Logger.getLogger(MyClientSideAsyncDecoder.class);
	static final byte RESP_TYPE_REPL = (byte) 0x00;
	static final byte RESP_TYPE_ERR = (byte) 0xff;
	static final byte RESP_TYPE_EOF = (byte) 0xfe;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf inBigEndian, List<Object> out) throws Exception {
        try {
            super.decode(ctx, inBigEndian, out);
        } catch (Exception e){
            logger.warn("Problem decoding/dispatching packet",e);
        }
    }

    @Override
	MyMessage instantiateMessage(ByteBuf frame) {
		MyMessage nativeMsg;
		MyMessageType mt;
        byte typeID = frame.readByte();
        switch (typeID) {
		case RESP_TYPE_REPL:
			mt = MyMessageType.REPL_EVENT_RESPONSE;
			break;
		case RESP_TYPE_ERR:
			mt = MyMessageType.ERROR_RESPONSE;
			break;
		case RESP_TYPE_EOF:
			mt = MyMessageType.EOFPKT_RESPONSE;
			break;
		default:
			 mt = MyMessageType.UNKNOWN;
			break;
		}

		if (logger.isDebugEnabled())
			logger.debug("Decoding message of type " + mt.toString());

		// create a new instance of the appropriate message
		nativeMsg = super.newResponseInstance(mt);

		return nativeMsg;
	}
}
