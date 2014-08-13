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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteOrder;

/**
 *
 * mysql packets come in two flavors, normal and extended.  Normal packets contain a header, which is the payload length and
 * sequence number, and then the payload.  A message that is (2^24-1) or larger can be sent by repeatedly sending packets
 * with increasing sequence numbers, until a final packet is received with a length shorter than (2^24 -1).
 * <br/>
 * To send an exact multiple of (2^24-1), the next packet should have a payload length of zero.
 * <br/>
 * HEADER, SHORT
 * HEADER, MAX, HEADER, MAX, HEADER, SHORT
 *
 */
public class Packet {
    public enum Modifier {HEAPCOPY_ON_READ}
    static final int HEADER_LENGTH = 4;
    static final int MAX_PAYLOAD = 0xffffff;

    final ByteBuf header;
    final CompositeByteBuf payload;
    Modifier modifier;
    int totalFullBytes = 0;
    boolean sealed = false;

    public Packet(ByteBufAllocator alloc) {
        this(alloc, null);
    }

    public Packet(ByteBufAllocator alloc, Modifier mod) {
        this.modifier = mod;
        if (modifier == Modifier.HEAPCOPY_ON_READ) {
            header = Unpooled.buffer(4,4).order(ByteOrder.LITTLE_ENDIAN);
            payload = Unpooled.compositeBuffer(50);
        } else {
            header = alloc.ioBuffer(4,4).order(ByteOrder.LITTLE_ENDIAN);
            payload = alloc.compositeBuffer(50);
        }
    }

    public int getPayloadLength(){
        return payload.readableBytes();
    }

    public int getSequenceNumber(){
        return header.getByte(3);
    }

    public ByteBuf unwrapHeader(){
        return header;
    }

    public ByteBuf unwrapPayload(){
        if (modifier == Modifier.HEAPCOPY_ON_READ){
            int maxCapacity = payload.readableBytes();
            ByteBuf copy = Unpooled.buffer(maxCapacity, maxCapacity);
            copy.writeBytes(payload);
            return copy;
        } else
            return payload;
    }

    public void release(){
        ReferenceCountUtil.release(header);
        ReferenceCountUtil.release(payload);
    }

    public boolean decodeMore(ByteBufAllocator alloc, ByteBuf input){
        if (! sealed ) {
            int transferToHeader = Math.min(header.writableBytes(),input.readableBytes());
            input.readBytes(header,transferToHeader);

            if (header.readableBytes() < HEADER_LENGTH){
                return false;//we don't have enough to read the header.
            }

            int chunkLength = header.getUnsignedMedium(0);
            int chunkAlreadyReceived = payload.writerIndex() - totalFullBytes;
            int chunkExpecting = chunkLength - chunkAlreadyReceived;
            int payloadTransfer = Math.min(chunkExpecting,input.readableBytes());

            ByteBuf slice = input.readSlice(payloadTransfer).retain();

            payload.addComponent(slice);
            payload.writerIndex( payload.writerIndex() + slice.readableBytes() ); //need to move the writer index, doesn't happen automatically.

            //recalculate how much we are expecting for this chunk.
            chunkAlreadyReceived = payload.writerIndex() - totalFullBytes;
            chunkExpecting = chunkLength - chunkAlreadyReceived;

            if (chunkExpecting == 0){
                //finished this packet, mark how many full packet bytes we've read.
                totalFullBytes = payload.writerIndex();
                if (chunkLength < MAX_PAYLOAD){
                    sealed = true;
                } else {
                    //an extended packet was indicated, prepare the read of the next packet.
                    header.clear();
                }
            }
        }
        return sealed;
    }

}
