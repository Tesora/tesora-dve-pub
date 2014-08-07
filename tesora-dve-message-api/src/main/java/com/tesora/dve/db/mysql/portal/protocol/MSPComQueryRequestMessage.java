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
import io.netty.buffer.Unpooled;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class MSPComQueryRequestMessage extends BaseMSPMessage {
    private static final int MAX_MYSQL_PAYLOAD_SIZE = 0xFFFFFF; //16777215=(2 raised to the 24th, minus 1).
    public static final byte TYPE_IDENTIFIER = (byte) 0x03;

    public boolean alreadySequenced = false;

    public MSPComQueryRequestMessage() {
        super();
    }

    public MSPComQueryRequestMessage(byte sequenceID, byte[] heapData) {
        super(sequenceID, heapData);
    }

    public MSPComQueryRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComQueryRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        final byte messageType = source.readByte();
        source = source.slice();
        return new MSPComQueryRequestMessage(sequenceID,source);
    }

    public byte[] getQueryBytes() {
        return MysqlAPIUtils.unwrapOrCopyReadableBytes(readBuffer());
    }

    public ByteBuf getQueryNative(){
        return readBuffer().slice();
    }

    public static MSPComQueryRequestMessage newMessage(byte sequenceID, String query, Charset encoding){
        ByteBuf buf = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
        buf.writeBytes(query.getBytes(encoding));
        return new MSPComQueryRequestMessage(sequenceID,buf);
    }

    @Override
    public void writeTo(ByteBuf destination) {
        ByteBuf sliceContents = readBuffer().slice().order(ByteOrder.LITTLE_ENDIAN);
        int sequenceIter = this.getSequenceID();
        int sendingPayloadSize;
        boolean fullPacket = false;
        do {
            int initialSize = sliceContents.readableBytes();
            int maxSlice = sequenceIter == 0 ? MAX_MYSQL_PAYLOAD_SIZE - 1 : MAX_MYSQL_PAYLOAD_SIZE;
            sendingPayloadSize = Math.min(maxSlice, initialSize);//will send a zero payload packet if packet is exact multiple of MAX_MYSQL_PAYLOAD_SIZE.
            fullPacket = (sendingPayloadSize == maxSlice);
            ByteBuf nextChunk = sliceContents.readSlice(sendingPayloadSize);
            MSPComQueryRequestMessage queryMsg = new MSPComQueryRequestMessage((byte)sequenceIter,nextChunk);
            queryMsg.defaultWriteTo(destination, sequenceIter != 0);
            sequenceIter++;
        } while (sliceContents.readableBytes() > 0 || fullPacket);
    }

}
