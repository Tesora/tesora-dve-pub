// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

public class MSPComFieldListRequestMessage extends BaseMSPMessage {

    public MSPComFieldListRequestMessage() {
        super();
    }

    public MSPComFieldListRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x04;
    }

    @Override
    public MSPComFieldListRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComFieldListRequestMessage(sequenceID,source);
    }
}
