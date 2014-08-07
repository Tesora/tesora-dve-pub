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

import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.db.mysql.libmy.MyNullBitmap;
import com.tesora.dve.db.mysql.libmy.MyParameter;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import io.netty.buffer.ByteBuf;
import java.nio.ByteOrder;
import java.util.List;

public class MSPComStmtExecuteRequestMessage extends BaseMSPMessage<MSPComStmtExecuteRequestMessage.ParsedData> {
    public static final MSPComStmtExecuteRequestMessage PROTOTYPE = new MSPComStmtExecuteRequestMessage();
    public static final byte TYPE_IDENTIFIER = (byte) 0x17;


    static class ParsedData {
        long statementID;
        byte flags;
        long iterationCount;
        int metadataOffset;

        MyPreparedStatement<?> metadata;
        List<Object> values;
    }

    protected MSPComStmtExecuteRequestMessage() {
        super();
    }

    protected MSPComStmtExecuteRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    protected MSPComStmtExecuteRequestMessage(byte sequenceID, ParsedData data) {
        super(sequenceID,data);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComStmtExecuteRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        source = source.slice();
        return new MSPComStmtExecuteRequestMessage(sequenceID,source);
    }

    @Override
    protected ParsedData unmarshall(ByteBuf source) {
        ParsedData parseValues = new ParsedData();
        source.skipBytes(1);//skip the type identifier.
        parseValues.statementID = source.readUnsignedInt();
        parseValues.flags = source.readByte();
        parseValues.iterationCount = source.readUnsignedInt();
        parseValues.metadataOffset = source.readerIndex();

        //we don't actually unmarshall the metadata and values here, since we don't know how many parameters there are.
        parseValues.metadata = null;
        parseValues.values = null;
        return parseValues;
    }

    @Override
    protected void marshall(ParsedData state, ByteBuf destination) {
        ByteBuf leBuf = destination.order(ByteOrder.LITTLE_ENDIAN);
        leBuf.writeByte(TYPE_IDENTIFIER);
        leBuf.writeInt((int) state.statementID);
        leBuf.writeByte(state.flags);
        leBuf.writeInt((int)state.iterationCount);
        if (state.metadata == null)
            throw new IllegalStateException("Cannot build execute request, no prepare metadata provided.");

        int numParams = state.metadata.getNumParams();
        if (numParams > 0) {
            MyNullBitmap nullBitmap = new MyNullBitmap(numParams, MyNullBitmap.BitmapType.EXECUTE_REQUEST);
            int bitmapIndex = leBuf.writerIndex();
            leBuf.writeZero(nullBitmap.length());
            if (state.metadata.isNewParameterList()) {
                leBuf.writeByte(1);
                for(MyParameter param : state.metadata.getParameters()) {
                    leBuf.writeByte(param.getType().getByteValue());
                    leBuf.writeZero(1);				}
            } else {
                leBuf.writeZero(1);
            }
            List<Object> params = state.values;
            for (int i = 0; i < params.size(); ++i) {
                if (params.get(i) != null)
                    DBTypeBasedUtils.getJavaTypeFunc(params.get(i).getClass()).writeObject(leBuf, params.get(i));
                else
                    nullBitmap.setBit(i+1);
            }
            leBuf.setBytes(bitmapIndex, nullBitmap.getBitmapArray());
        }
    }

    public long getStatementID() {
        return readState().statementID;
    }

    public void readParameterMetadata(MyPreparedStatement<String> pStmt) throws PEException {
        //TODO: this belongs in unmarshall, but requires knowledge about the expected number of parameters, provided by a previous prepare response.
        ParsedData cachedParse = readState();
        ByteBuf backingData = readBuffer();
        int lengthOfMetadata = backingData.readableBytes() - cachedParse.metadataOffset;
        ByteBuf in = backingData.slice(cachedParse.metadataOffset, lengthOfMetadata).order(ByteOrder.LITTLE_ENDIAN);
        if (pStmt.getNumParams() > 0) {
            int nullBitmapLength = (pStmt.getNumParams() + 7) / 8;
            MyNullBitmap nullBitmap = new MyNullBitmap(MysqlAPIUtils.readBytes(in, nullBitmapLength),
                    pStmt.getNumParams(), MyNullBitmap.BitmapType.EXECUTE_REQUEST);
            if (in.readByte() == 1) { // this is new params bound flag; only
                // =1 on first stmt_execute
                pStmt.clearParameters();
                for (int i = 0; i < pStmt.getNumParams(); i++) {
                    pStmt.addParameter(new MyParameter(MyFieldType
                            .fromByte(in.readByte())));
                    in.skipBytes(1);
                }
            }

            for (int paramNum = 1; paramNum <= pStmt.getNumParams(); paramNum++) {
                MyParameter parameter = pStmt.getParameter(paramNum);
                if (nullBitmap.getBit(paramNum)) {
                    parameter.setValue(null);
                } else {
                    //TODO - may need to figure out how to get the proper flags and length into this call
                    parameter.setValue(DBTypeBasedUtils.getMysqlTypeFunc(parameter.getType()).readObject(in));
                }
            }
        }
    }

    public static MSPComStmtExecuteRequestMessage newMessage(int statementID, MyPreparedStatement<MysqlGroupedPreparedStatementId> metadata, List<Object> values) throws PEException {
        if (metadata.getNumParams() > 0 && metadata.getNumParams() != values.size())
            throw new PECodingException("Wrong number of parameters specified for prepared statement execution (expected"
                    + metadata.getNumParams() + ", received " + values.size() + ")");

        ParsedData data = new ParsedData();
        data.statementID = statementID;
        data.flags = 0;
        data.iterationCount = 1;
        data.metadata = metadata;
        data.values = values;
        return new MSPComStmtExecuteRequestMessage((byte)0,data);
    }

}
