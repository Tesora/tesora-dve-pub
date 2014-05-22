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

import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
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

    class RowPacketData {
        public RowPacketData(MyBinaryResultRow binRow, long[] autoIncr) {
            this.binRow = binRow;
            this.autoIncr = autoIncr;
        }

        MyBinaryResultRow binRow;
        long[] autoIncr = null;
    }

    int stmtID = -1;
    ColumnSet rowSetMetadata;
    ByteBuf metadataFragment = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
    int columnsPerTuple;
    boolean needsNewParams;
    List<RowPacketData> queuedPackets = new ArrayList<>();


    public void setColumnsPerTuple(int columnsPerTuple) {
        this.columnsPerTuple = columnsPerTuple;
    }

    public void setRowSetMetadata(ColumnSet rowSetMetadata) {
        this.rowSetMetadata = rowSetMetadata;
        this.metadataFragment.clear();
        for (ColumnMetadata colMetadata : rowSetMetadata.getColumnList()) {
            if (colMetadata.getNativeTypeId() == MyFieldType.FIELD_TYPE_INT24.getByteValue())
                metadataFragment.writeByte(MyFieldType.FIELD_TYPE_LONG.getByteValue());
            else
                metadataFragment.writeByte(colMetadata.getNativeTypeId());
            if ((colMetadata.getNativeTypeFlags() & MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED) > 0)
                metadataFragment.writeByte(0x80);
            else
                metadataFragment.writeZero(1);
        }
    }


    public void setStmtID(int stmtID) {
        this.stmtID = stmtID;
    }

    public void add(MyBinaryResultRow binRow, long[] autoIncrBlocks) {
        queuedPackets.add(new RowPacketData(binRow, autoIncrBlocks));
    }

    public void add(RowPacketData rowPacketData) {
        queuedPackets.add(rowPacketData);
    }

    public boolean isEmpty() {
        return queuedPackets.isEmpty();
    }

    public int size() {
        return queuedPackets.size();
    }

    public int getSizeInBytes() {
        int initialCapacity = 0;
        for (RowPacketData packetData : queuedPackets) {
            initialCapacity += packetData.binRow.sizeInBytes();
        }
        return initialCapacity;
    }

    public void marshallMessage(ByteBuf stmtExecuteBuf) throws PEException {
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
        for (Iterator<RowPacketData> i = this.queuedPackets.iterator(); i.hasNext(); ) {
            RowPacketData rowPacketData = i.next();

//                ByteBuf rowSet = Unpooled.buffer(rowPacketData.binRow.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
//                rowPacketData.binRow.marshallFullMessage(rowSet);

            long[] autoIncrBlocks = rowPacketData.autoIncr;

//                while (rowSet.isReadable() && rowsToWrite-- > 0) {
//				System.out.println(siteCtx + "/" + myi + ": adding row");
//                    int payloadLen = rowSet.readMedium();
//                    rowSet.skipBytes(1);
//                    byte packetHeader = rowSet.readByte();
//                    if (packetHeader != 0)
//                        throw new PEException("Out-of-sync reading redist rowSet");
            int bitmapSize = MyNullBitmap.computeSize(columnsPerTuple, MyNullBitmap.BitmapType.RESULT_ROW);
            int rowFields = rowPacketData.binRow.size();
            for (int colIndex = 1; colIndex <= columnsPerTuple; colIndex++, execStmtColIndex++) {
                //we are looping through target columns, which may exceed the source column count.
                if ((colIndex <= rowFields) && rowPacketData.binRow.isNull(colIndex - 1 /* zero based indexing */))
                    nullBitmap.setBit(execStmtColIndex);
            }
//                    rowSet.skipBytes(bitmapSize);

//                    stmtExecuteBuf.writeBytes(rowSet, payloadLen-bitmapSize-1);
            rowPacketData.binRow.marshallRawValues(stmtExecuteBuf);
            if (autoIncrBlocks != null) {
                stmtExecuteBuf.writeLong(autoIncrBlocks[0]++);
            }
            ++rowsWritten;
//                }
//                if (!rowSet.isReadable()) {
//                    i.remove();
//                    rowSet.release();
//                }

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
