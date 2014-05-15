// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

public class MSPComStatisticsRequestMessage extends BaseMSPMessage {
    public MSPComStatisticsRequestMessage() {
        super();
    }

    public MSPComStatisticsRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x09;
    }

    @Override
    public MSPComStatisticsRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComStatisticsRequestMessage(sequenceID,source);
    }
}
