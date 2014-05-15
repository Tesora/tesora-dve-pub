// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class MSPComInitDBRequestMessage extends BaseMSPMessage<String> {
    Charset decodingCharset;

    public MSPComInitDBRequestMessage() {
        super();
    }

    public MSPComInitDBRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x02;
    }

    @Override
    public MSPComInitDBRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComInitDBRequestMessage(sequenceID,source);
    }

    public void setDecodingCharset(Charset javaCharset){
        this.decodingCharset = javaCharset;
    }

    public String getInitialDatabase() {
        return readState();
    }

    @Override
    protected String unmarshall(ByteBuf source) {
        if (decodingCharset == null)
            throw new IllegalStateException("initDB request cannot unmarshall a packet without a decoding charset.");

        return source.toString(decodingCharset);
    }

    @Override
    protected void marshall(String state, ByteBuf destination) {
        throw new UnsupportedOperationException();
    }

}
