// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;

public class MSPComPrepareStmtRequestMessage extends BaseMSPMessage<String> {
    public static final byte TYPE_IDENTIFIER = (byte) 0x16;

    Charset decodingCharset;

    public MSPComPrepareStmtRequestMessage() {
        super();
    }

    public MSPComPrepareStmtRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComPrepareStmtRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComPrepareStmtRequestMessage(sequenceID,source);
    }

    public Charset getDecodingCharset() {
        return decodingCharset;
    }

    public void setDecodingCharset(Charset decodingCharset) {
        this.decodingCharset = decodingCharset;
    }

    public String getPrepareSQL(){
        return readState();
    }

    public void setPrepareSQL(String sql){
        set(sql);
    }

    @Override
    protected String unmarshall(ByteBuf source) {
        return source.toString(decodingCharset);
    }

    @Override
    protected void marshall(String state, ByteBuf destination) {
        destination.writeBytes( decodingCharset.encode( state ));
    }

    public byte[] getPrepareBytes() {
        return MysqlAPIUtils.unwrapOrCopyReadableBytes(readBuffer());
    }

    public static MSPComPrepareStmtRequestMessage newMessage(byte sequenceID, String sql, Charset charset) {
        MSPComPrepareStmtRequestMessage prepStmt = new MSPComPrepareStmtRequestMessage();
        prepStmt.setSequenceID(sequenceID);
        prepStmt.setDecodingCharset(charset);
        prepStmt.setPrepareSQL(sql);
        return prepStmt;
    }

}
