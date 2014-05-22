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

import com.tesora.dve.db.mysql.common.DataTypeValueFunc;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteOrder;
import java.util.List;

public class MyBinaryResultRow extends MyResponseMessage {
    static final int BINARY_MARKER_LENGTH = 1;
    static final int NULL_BITMAP_OFFSET = BINARY_MARKER_LENGTH;

    ByteBuf backingBuffer = Unpooled.EMPTY_BUFFER;
    List<DataTypeValueFunc> fieldConverters;
    ByteBuf[] fieldSlices;

    public MyBinaryResultRow(List<DataTypeValueFunc> fieldConverters) {
        this.fieldConverters = fieldConverters;
        this.fieldSlices = new ByteBuf[fieldConverters.size()];
    }

    @Override
    public MyMessageType getMessageType() {
        throw new PECodingException(MyBinaryResultRow.class.getSimpleName());
    }

    @Override
    public void marshallMessage(ByteBuf cb) {
        cb.writeBytes(backingBuffer.slice());
    }


    public void marshallRawValues(ByteBuf cb) {
        int expectedBitmapLength = MyNullBitmap.computeSize(fieldConverters.size(),MyNullBitmap.BitmapType.RESULT_ROW);
        ByteBuf values = backingBuffer.slice().skipBytes(BINARY_MARKER_LENGTH + expectedBitmapLength).order(ByteOrder.LITTLE_ENDIAN);
        cb.writeBytes(values);
    }

    @Override
    public void unmarshallMessage(ByteBuf cb) throws PEException {
        backingBuffer = Unpooled.buffer(cb.readableBytes()).writeBytes(cb);
        int expectedFieldCount = fieldConverters.size();
        int expectedBitmapLength = MyNullBitmap.computeSize(expectedFieldCount,MyNullBitmap.BitmapType.RESULT_ROW);

        byte[] nullBitmap = new byte[expectedBitmapLength];
        backingBuffer.getBytes(NULL_BITMAP_OFFSET,nullBitmap);
        MyNullBitmap resultBitmap = new MyNullBitmap(nullBitmap,expectedFieldCount, MyNullBitmap.BitmapType.RESULT_ROW);

        ByteBuf values = backingBuffer.slice().skipBytes(BINARY_MARKER_LENGTH + expectedBitmapLength).order(ByteOrder.LITTLE_ENDIAN);

        for (int i=0;i < expectedFieldCount;i++){
            ByteBuf existing = fieldSlices[i];
            ByteBuf nextSlice = null;
            int startIndex = values.readerIndex();
            if ( ! resultBitmap.getBit(i + 1) ) {
                fieldConverters.get(i).readObject(values);//TODO: we throw out the unmarshalled value, we could cache it.
                int endingOffset = values.readerIndex();
                nextSlice = values.slice(startIndex,endingOffset - startIndex);
            }

            if (existing != null)
                existing.release();
            fieldSlices[i] = nextSlice;
        }
    }

    public int size(){
        return fieldSlices.length;
    }

    public ByteBuf getSlice(int itemNumber) {
        ByteBuf field = fieldSlices[itemNumber];
        if (field == null)
            return null;
        else
            return field.slice();
    }

    public boolean isNull(int itemNumber){
        return fieldSlices[itemNumber] == null;
    }

    public Object getValue(int itemNumber) throws PEException {
        ByteBuf slice = getSlice(itemNumber);
        if (slice == null)
            return null;
        else
            return fieldConverters.get(itemNumber).readObject(slice);
    }

    public int sizeInBytes() {
        return backingBuffer.readableBytes() + super.MESSAGE_HEADER_LENGTH;
    }
}
