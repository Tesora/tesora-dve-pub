// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MSPComQuitRequestMessage extends BaseMSPMessage {

    public static final byte TYPE_IDENTIFIER = (byte) 0x01;

    public MSPComQuitRequestMessage() {
        super((byte)0, Unpooled.buffer());
    }

    public MSPComQuitRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComQuitRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComQuitRequestMessage(sequenceID,source);
    }

}
