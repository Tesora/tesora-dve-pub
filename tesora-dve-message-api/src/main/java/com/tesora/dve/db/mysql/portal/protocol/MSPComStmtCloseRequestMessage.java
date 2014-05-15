// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;

public class MSPComStmtCloseRequestMessage extends BaseMSPMessage<Long> {
    static final int INDEX_OF_STATEMENTID = 0;
    public static final byte TYPE_IDENTIFIER = (byte) 0x19;

    public MSPComStmtCloseRequestMessage() {
        super();
    }

    public MSPComStmtCloseRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComStmtCloseRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComStmtCloseRequestMessage(sequenceID,source);
    }

    public long getStatementID() {
        return super.readState();
    }

    public void setStatementID(long statementID) {
        super.set(statementID);
    }

    @Override
    protected Long unmarshall(ByteBuf source) {
        return source.getUnsignedInt(INDEX_OF_STATEMENTID);
    }

    @Override
    protected void marshall(Long statementID, ByteBuf destination) {
        destination.writeInt(statementID.intValue());
    }

    public static MSPComStmtCloseRequestMessage newMessage(byte sequenceID, int pstmtId) {
        MSPComStmtCloseRequestMessage closeReq = new MSPComStmtCloseRequestMessage();
        closeReq.setSequenceID(sequenceID);
        closeReq.setStatementID(pstmtId);
        return closeReq;
    }

}
