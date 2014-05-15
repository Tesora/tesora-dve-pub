// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteOrder;

public class MSPProtocolEncoder extends MessageToByteEncoder<MSPMessage> {

    public MSPProtocolEncoder() {
        super(MSPMessage.class);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, MSPMessage msg, ByteBuf out) throws Exception {
        ByteBuf littleEnd = out.order(ByteOrder.LITTLE_ENDIAN);
        msg.writeTo(littleEnd);
    }
}
