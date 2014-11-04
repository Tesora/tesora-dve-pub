package com.tesora.dve.db.mysql.libmy;

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

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class BufferedExecute extends MyMessage {
    int stmtID = -1;
    ByteBuf metadataFragment = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
    int columnsPerTuple;
    boolean needsNewParams;
    List<MyBinaryResultRow> queuedPackets = new ArrayList<>();


    public void setColumnsPerTuple(int columnsPerTuple) {
        this.columnsPerTuple = columnsPerTuple;
    }

    public void setStmtID(int stmtID) {
        this.stmtID = stmtID;
    }

    public void add(MyBinaryResultRow binRow) {
        generateMetadataIfNeeded(binRow);
        queuedPackets.add(binRow);
    }

    private void generateMetadataIfNeeded(MyBinaryResultRow binRow) {
        if (metadataFragment.readableBytes() <= 0) {
            for (int i = 0; i < binRow.size(); i++) {
                DecodedMeta valueFunction = binRow.getValueFunction(i);
                metadataFragment.writeByte(valueFunction.getNativeTypeForExecute());
                if (valueFunction.isUnsigned()) //per the docs, cursor flag has high bit set for unsigned values.
                    metadataFragment.writeByte(0x80);
                else
                    metadataFragment.writeZero(1);
            }
        }

    }

    public boolean isEmpty() {
        return queuedPackets.isEmpty();
    }

    public int size() {
        return queuedPackets.size();
    }

    public int getSizeInBytes() {
        int initialCapacity = 0;
        for (MyBinaryResultRow binRow : queuedPackets) {
            initialCapacity += binRow.sizeInBytes();
        }
        return initialCapacity;
    }

    public void marshallMessage(ByteBuf stmtExecuteBuf) {
        //header and type are marshalled by parent class.

        int rowsToFlush = this.size();

        MyNullBitmap nullBitmap = new MyNullBitmap(rowsToFlush * columnsPerTuple, MyNullBitmap.BitmapType.EXECUTE_REQUEST);

//            stmtExecuteBuf.writeByte(MSPComStmtExecuteRequestMessage.TYPE_IDENTIFIER);
        stmtExecuteBuf.writeInt(this.stmtID);
        stmtExecuteBuf.writeZero(1); // flags
        stmtExecuteBuf.writeInt(1); // iteration count
        int nullBitmapIndex = stmtExecuteBuf.writerIndex();
        stmtExecuteBuf.writeZero(nullBitmap.length());

        // write the parameter types, as appropriate
        if (this.needsNewParams) {
            stmtExecuteBuf.writeByte(1);

            for (int i = 0; i < rowsToFlush; ++i) {
                stmtExecuteBuf.writeBytes(metadataFragment.slice());
            }


        } else
            stmtExecuteBuf.writeZero(1);

        // Copy the parameter values, updating the null bitmap
        // null bitmap is 1-based
        int rowsWritten = 0;
        int execStmtColIndex = 1;
        for (Iterator<MyBinaryResultRow> i = this.queuedPackets.iterator(); i.hasNext(); ) {
            MyBinaryResultRow rowPacketData = i.next();

//                ByteBuf rowSet = Unpooled.buffer(rowPacketData.binRow.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
//                rowPacketData.binRow.marshallFullMessage(rowSet);

            //                while (rowSet.isReadable() && rowsToWrite-- > 0) {
//				System.out.println(siteCtx + "/" + myi + ": adding row");
//                    int payloadLen = rowSet.readMedium();
//                    rowSet.skipBytes(1);
//                    byte packetHeader = rowSet.readByte();
//                    if (packetHeader != 0)
//                        throw new PEException("Out-of-sync reading redist rowSet");
            int bitmapSize = MyNullBitmap.computeSize(columnsPerTuple, MyNullBitmap.BitmapType.RESULT_ROW);
            int rowFields = rowPacketData.size();
            for (int colIndex = 1; colIndex <= columnsPerTuple; colIndex++, execStmtColIndex++) {
                //we are looping through target columns, which may exceed the source column count.
                if ((colIndex <= rowFields) && rowPacketData.isNull(colIndex - 1 /* zero based indexing */))
                    nullBitmap.setBit(execStmtColIndex);
            }
//                    rowSet.skipBytes(bitmapSize);

//                    stmtExecuteBuf.writeBytes(rowSet, payloadLen-bitmapSize-1);
            rowPacketData.marshallRawValues(stmtExecuteBuf);
            ++rowsWritten;

        }

        if (rowsWritten != rowsToFlush) {
//			System.out.println("At failure " + stmtExecuteBuf + "/" + siteCtx);
            throw new PECodingException("Asked to write " + rowsToFlush + " rows, but only " + rowsWritten + " were (" + rowsToFlush + " were made available to flushBuffers)");
        }

        // Go back and set the null bitmap and the payload size
        stmtExecuteBuf.setBytes(nullBitmapIndex, nullBitmap.getBitmapArray());
    }

    public void setNeedsNewParams(boolean needsNewParams) {
        this.needsNewParams = needsNewParams;
    }

    @Override
    public MyMessageType getMessageType() {
        return MyMessageType.COM_STMT_EXECUTE_REQUEST;
    }


    @Override
    public boolean isMessageTypeEncoded() {
        return true;
    }

    @Override
    public void unmarshallMessage(ByteBuf cb) throws PEException {
        throw new PECodingException("Unmarshall unsupported for this type, " + this.getClass().getSimpleName());
    }
}
