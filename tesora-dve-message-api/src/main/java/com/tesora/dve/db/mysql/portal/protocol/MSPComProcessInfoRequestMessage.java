// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

public class MSPComProcessInfoRequestMessage extends BaseMSPMessage {

    public MSPComProcessInfoRequestMessage() {
        super();
    }

    public MSPComProcessInfoRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x0a;
    }

    @Override
    public MSPComProcessInfoRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComProcessInfoRequestMessage(sequenceID,source);
    }
}