// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PECodingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class MyTextResultRow extends MyResponseMessage {
    ByteBuf backingBuffer = Unpooled.EMPTY_BUFFER;
    int[] stringOffsets = new int[]{0};

    public MyTextResultRow() {
    }

    @Override
    public MyMessageType getMessageType() {
        throw new PECodingException(MyTextResultRow.class.getSimpleName());
    }

    @Override
    public void marshallMessage(ByteBuf cb) {
        cb.writeBytes(backingBuffer.slice());
    }

    @Override
    public void unmarshallMessage(ByteBuf cb) {
        backingBuffer = Unpooled.buffer(cb.readableBytes()).writeBytes(cb).order(ByteOrder.LITTLE_ENDIAN);
        ArrayList<Integer> offsets = MysqlAPIUtils.locateLengthCodedStrings(backingBuffer);
        stringOffsets = new int[offsets.size() + 1];
        for (int i=0;i< offsets.size();i++)
            stringOffsets[i] = offsets.get(i);
        stringOffsets[offsets.size()] = backingBuffer.readableBytes();//last entry is (endpoint+1) of the last string.
    }

    public int size(){
        return stringOffsets.length - 1;
    }

    public ByteBuf getSlice(int itemNumber) {
        int startOffset = stringOffsets[itemNumber];
        int endingOffset = stringOffsets[itemNumber+1];
        return backingBuffer.slice(startOffset, endingOffset - startOffset);
    }

    public byte[] getBytes(int itemNumber){
        return MysqlAPIUtils.getLengthCodedBinary(getSlice(itemNumber));
    }

    public String getString(int itemNumber){
        return MysqlAPIUtils.getLengthCodedString(getSlice(itemNumber));
    }

    public String getString(int itemNumber, Charset decoder){
        return MysqlAPIUtils.getLengthCodedString(getSlice(itemNumber), decoder);
    }




}
