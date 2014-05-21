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

import com.tesora.dve.db.mysql.libmy.MyProtocolDefs;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteOrder;

public class MSPServerGreetingRequestMessage extends BaseMSPMessage implements MSPUntypedMessage {
    public static final byte MYSQL_PROTOCOL_VERSION=10;

    public MSPServerGreetingRequestMessage() {
        super();
    }

    public MSPServerGreetingRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    public static void write(ChannelHandlerContext ctx, int connectionId, String salt, int serverCapabilities, String serverVersion, byte serverCharSet, String pluginData) {
        ByteBuf out = ctx.channel().alloc().heapBuffer(50).order(ByteOrder.LITTLE_ENDIAN);


        String scrambleBuffer1st = salt.substring(0,8);
        String scrambleBuffer2nd = salt.substring(8) + '\0';
        Integer scrambleBufferSize = scrambleBuffer1st.length() + scrambleBuffer2nd.length();

        ByteBuf serverCapabilitiesBuf = ctx .channel().alloc().heapBuffer(4).order(ByteOrder.LITTLE_ENDIAN);
        try {
        	serverCapabilitiesBuf.writeInt(serverCapabilities);
            out.writeMedium(0);
            out.writeByte(0);
            out.writeByte(MYSQL_PROTOCOL_VERSION);

            out.writeBytes(serverVersion.getBytes());
            out.writeZero(1);
            out.writeInt(connectionId);
            out.writeBytes(scrambleBuffer1st.getBytes()); // Salt
            out.writeZero(1);
            out.writeByte(serverCapabilitiesBuf.getByte(0));
            out.writeByte(serverCapabilitiesBuf.getByte(1));

            out.writeByte(serverCharSet);
            out.writeShort(MyProtocolDefs.SERVER_STATUS_AUTOCOMMIT);
            out.writeByte(serverCapabilitiesBuf.getByte(2));
            out.writeByte(serverCapabilitiesBuf.getByte(3));
            out.writeByte(scrambleBufferSize.byteValue());
            out.writeZero(10); // write 10 unused bytes
            out.writeBytes(scrambleBuffer2nd.getBytes()); // Salt

            out.writeBytes(pluginData.getBytes()); // payload
            out.writeZero(1);

            out.setMedium(0, out.writerIndex()-4);

            ctx.channel().write(out);
            ctx.flush();
        } finally {
            serverCapabilitiesBuf.release();
        }
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0xD0;
    }

    @Override
    public MSPServerGreetingRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPServerGreetingRequestMessage(sequenceID,source);
    }
}
