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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class MyBinaryResultRow extends MyResponseMessage {
    static final Logger log = LoggerFactory.getLogger(MyBinaryResultRow.class);

    //TODO: need to figure out a way to reduce downstream need to collect rowset headers. introduce flags and allow autoinc modifications? -sgossard
    List<DecodedMeta> fieldConverters;
    List<ByteBuf> fieldSlices;

    public MyBinaryResultRow(List<DecodedMeta> fieldConverters) {
        this.fieldConverters = fieldConverters;
        this.fieldSlices = new ArrayList<ByteBuf>();
        for (int i=0; i< fieldConverters.size();i++){
            this.fieldSlices.add(null);
        }
    }

    protected MyBinaryResultRow(List<DecodedMeta> fieldConverters, List<ByteBuf> fieldSlices) {
        this.fieldConverters = fieldConverters;
        this.fieldSlices = fieldSlices;
    }

    @Override
    public MyMessageType getMessageType() {
        throw new PECodingException(MyBinaryResultRow.class.getSimpleName());
    }

    public MyBinaryResultRow append(DecodedMeta valueFunc, Object value){
        ByteBuf valueBuf = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
        valueFunc.writeObject(valueBuf,value);
        return this.append(valueFunc,valueBuf);
    }

    public MyBinaryResultRow append(DecodedMeta valueFunc, ByteBuf fieldSlice){
        List<DecodedMeta> copyOfFuncs = new ArrayList<>(this.fieldConverters);
        copyOfFuncs.add(valueFunc);

        List<ByteBuf> newFields = new ArrayList<>(this.fieldSlices);
        newFields.add(fieldSlice);

        return new MyBinaryResultRow(copyOfFuncs,newFields);
    }

    @Override
    public void marshallMessage(ByteBuf cb) {
        cb.writeZero(1);//binary row marker
        byte[] bitmapArray = constructNullMap();
        cb.writeBytes(bitmapArray);
        marshallRawValues(cb);
    }

    private byte[] constructNullMap() {
        MyNullBitmap constructBitMap = new MyNullBitmap(fieldSlices.size(), MyNullBitmap.BitmapType.RESULT_ROW);
        for (int i=0;i<fieldSlices.size();i++){
            if (fieldSlices.get(i) == null)
                constructBitMap.setBit(i + 1);
        }
        return constructBitMap.getBitmapArray();
    }


    public void marshallRawValues(ByteBuf cb) {
        for (int i=0;i<fieldSlices.size();i++){
            ByteBuf fieldSlice = fieldSlices.get(i);
            if (fieldSlice != null)
                cb.writeBytes(fieldSlice.slice());
        }
    }

    @Override
    public void unmarshallMessage(ByteBuf cb) throws PEException {
        int expectedFieldCount = fieldConverters.size();
        int expectedBitmapLength = MyNullBitmap.computeSize(expectedFieldCount,MyNullBitmap.BitmapType.RESULT_ROW);
        cb = cb.order(ByteOrder.LITTLE_ENDIAN);
        cb.skipBytes(1);//skip the bin row marker.

        byte[] nullBitmap = new byte[expectedBitmapLength];
        cb.readBytes(nullBitmap);
        MyNullBitmap resultBitmap = new MyNullBitmap(nullBitmap,expectedFieldCount, MyNullBitmap.BitmapType.RESULT_ROW);

        ByteBuf values = cb;

        for (int i=0;i < expectedFieldCount;i++){
            ByteBuf existing = fieldSlices.get(i);
            ByteBuf nextSlice = null;
            int startIndex = values.readerIndex();
            if ( ! resultBitmap.getBit(i + 1) ) {
                fieldConverters.get(i).readObject(values);//TODO: we throw out the unmarshalled value, we could cache it.
                int endingOffset = values.readerIndex();
                nextSlice = values.slice(startIndex,endingOffset - startIndex);
            }

            if (existing != null)
                existing.release();
            fieldSlices.set(i,nextSlice);
        }
        if (cb.readableBytes() > 0) {
            log.warn("Decoded binary row had {} leftover bytes, re-encoding may fail.",cb.readableBytes());
            cb.skipBytes(cb.readableBytes());//consume rest of buffer.
        }
    }

    public MyBinaryResultRow projection(int[] desiredFields){
        int expectedFieldCount = desiredFields.length;

        ArrayList<ByteBuf> newSlices = new ArrayList<>(expectedFieldCount);
        ArrayList<DecodedMeta> newConverters = new ArrayList<>(expectedFieldCount);

        for (int targetIndex=0;targetIndex<expectedFieldCount;targetIndex++){
            int sourceIndex = desiredFields[targetIndex];
            newConverters.add(fieldConverters.get(sourceIndex));//use the source index, not the target index..
            if (fieldSlices.get(sourceIndex) == null) {
                newSlices.add(null);
            } else {
                ByteBuf fieldSlice = fieldSlices.get(sourceIndex);
                ByteBuf copySlice = Unpooled.buffer(fieldSlice.readableBytes()).order(ByteOrder.LITTLE_ENDIAN);
                copySlice.writeBytes(fieldSlice.slice());
                newSlices.add(copySlice);
            }
        }

        return new MyBinaryResultRow(newConverters, newSlices);
    }

    public int size(){
        return fieldSlices.size();
    }

    public DecodedMeta getValueFunction(int itemNumber){
        return fieldConverters.get(itemNumber);
    }

    public ByteBuf getSlice(int itemNumber) {
        ByteBuf field = fieldSlices.get(itemNumber);
        if (field == null)
            return null;
        else
            return field.slice();
    }

    public boolean isNull(int itemNumber){
        return fieldSlices.get(itemNumber) == null;
    }

    public Object getValue(int itemNumber) throws PEException {
        ByteBuf slice = getSlice(itemNumber);
        if (slice == null)
            return null;
        else
            return fieldConverters.get(itemNumber).readObject(slice);
    }

    public int sizeInBytes() {
        int totalSize = super.MESSAGE_HEADER_LENGTH;
        totalSize+= 1;
        totalSize+= MyNullBitmap.computeSize(fieldSlices.size(), MyNullBitmap.BitmapType.RESULT_ROW);
        for (int i=0;i<fieldSlices.size();i++){
            ByteBuf fieldSlice = fieldSlices.get(i);
            if (fieldSlice != null)
                totalSize+= fieldSlice.readableBytes();
        }
        return totalSize;
    }
}
