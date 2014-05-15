// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

public class MSPComSetOptionRequestMessage extends BaseMSPMessage<Short> {
    static final int INDEX_OF_OPTIONFLAG = 0;

    public MSPComSetOptionRequestMessage() {
        super();
    }

    public MSPComSetOptionRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x1b;
    }

    @Override
    protected Short unmarshall(ByteBuf source) {
        return source.getShort(INDEX_OF_OPTIONFLAG);
    }

    @Override
    public MSPComSetOptionRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComSetOptionRequestMessage(sequenceID,source);
    }

    public short getOptionFlag() {
        return readState();
    }


}
