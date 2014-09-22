package com.tesora.dve.db.mysql.portal.protocol;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.tesora.dve.db.mysql.NoResponseExpected;
import io.netty.buffer.ByteBuf;

public class MSPComStmtCloseRequestMessage extends BaseMSPMessage<Long> implements NoResponseExpected {
    public static final MSPComStmtCloseRequestMessage PROTOTYPE = new MSPComStmtCloseRequestMessage();
    static final int INDEX_OF_STATEMENTID = 0;
    public static final byte TYPE_IDENTIFIER = (byte) 0x19;

    protected MSPComStmtCloseRequestMessage() {
        super();
    }

    protected MSPComStmtCloseRequestMessage(ByteBuf backing) {
        super(backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComStmtCloseRequestMessage newPrototype(ByteBuf source) {
        source = source.slice();
        return new MSPComStmtCloseRequestMessage(source);
    }

    public long getStatementID() {
        return super.readState();
    }

    public void setStatementID(long statementID) {
        super.set(statementID);
    }

    @Override
    protected Long unmarshall(ByteBuf source) {
        source.skipBytes(1);
        return source.getUnsignedInt(INDEX_OF_STATEMENTID);
    }

    @Override
    protected void marshall(Long statementID, ByteBuf destination) {
        destination.writeByte(TYPE_IDENTIFIER);
        destination.writeInt(statementID.intValue());
    }

    public static MSPComStmtCloseRequestMessage newMessage(int pstmtId) {
        MSPComStmtCloseRequestMessage closeReq = new MSPComStmtCloseRequestMessage();
        closeReq.setStatementID(pstmtId);
        return closeReq;
    }

}
