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

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;

public class MSPComPrepareStmtRequestMessage extends BaseMSPMessage<String> {
    public static final MSPComPrepareStmtRequestMessage PROTOTYPE = new MSPComPrepareStmtRequestMessage();
    public static final byte TYPE_IDENTIFIER = (byte) 0x16;

    Charset decodingCharset;

    protected MSPComPrepareStmtRequestMessage() {
        super();
    }

    protected MSPComPrepareStmtRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComPrepareStmtRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        source = source.slice();
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
        source.skipBytes(1);//skip type field.
        return source.toString(decodingCharset);
    }

    @Override
    protected void marshall(String state, ByteBuf destination) {
        destination.writeByte( getMysqlMessageType() );
        destination.writeBytes( decodingCharset.encode( state ));
    }

    public byte[] getPrepareBytes() {
        return MysqlAPIUtils.unwrapOrCopyReadableBytes(getRemainingBuf());
    }

    private ByteBuf getRemainingBuf() {
        ByteBuf readBuf = readBuffer();
        ByteBuf remainingBuf = readBuf.slice(1,readBuf.readableBytes() - 1);//skip over type field.
        return remainingBuf;
    }

    public static MSPComPrepareStmtRequestMessage newMessage(byte sequenceID, String sql, Charset charset) {
        MSPComPrepareStmtRequestMessage prepStmt = new MSPComPrepareStmtRequestMessage();
        prepStmt.setSequenceID(sequenceID);
        prepStmt.setDecodingCharset(charset);
        prepStmt.setPrepareSQL(sql);
        return prepStmt;
    }

}
