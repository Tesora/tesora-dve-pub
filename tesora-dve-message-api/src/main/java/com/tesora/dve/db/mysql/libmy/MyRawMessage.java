// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MyRawMessage extends MyResponseMessage {
    ByteBuf heapBuffer = Unpooled.EMPTY_BUFFER;


    @Override
    public MyMessageType getMessageType() {
        return MyMessageType.UNKNOWN;
    }

    @Override
    public void marshallMessage(ByteBuf cb) {
        cb.writeBytes(heapBuffer.slice());
    }

    @Override
    public void unmarshallMessage(ByteBuf cb) {
        heapBuffer = Unpooled.buffer(cb.readableBytes());
        heapBuffer.writeBytes(cb);
    }

}
