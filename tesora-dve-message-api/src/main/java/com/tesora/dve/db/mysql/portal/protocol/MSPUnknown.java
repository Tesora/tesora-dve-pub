// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

public class MSPUnknown extends BaseMSPMessage {
    byte messageType;

    public MSPUnknown() {
        super();
        this.messageType = (byte)0xFF;
    }

    public MSPUnknown(byte messageType, byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
        this.messageType = messageType;
    }

    public MSPUnknown(byte sequenceID, ByteBuf backing) {
        this((byte) 0xFF, sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return messageType;
    }

    @Override
    public MSPUnknown newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPUnknown(sequenceID,source);
    }
}
