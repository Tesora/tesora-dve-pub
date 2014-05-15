// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

public class MSPComPingRequestMessage extends BaseMSPMessage {

    public MSPComPingRequestMessage() {
        super();
    }

    public MSPComPingRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x0e;
    }

    @Override
    public MSPComPingRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComPingRequestMessage(sequenceID,source);
    }
}
