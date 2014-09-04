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

import com.tesora.dve.db.mysql.MysqlMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    static final Logger logger = LoggerFactory.getLogger(Packet.class);
    public enum Modifier {HEAPCOPY_ON_READ}
    static final int HEADER_LENGTH = 4;
    static final int MAX_PAYLOAD = 0xffffff;

    final ByteBuf header;
    final CompositeByteBuf payload;
    Modifier modifier;
    int expectedSequence;
    int totalFullBytes = 0;
    boolean sealed = false;
    String context;

    public Packet(ByteBufAllocator alloc, int expectedSequence, Modifier mod, String context) {
        this.expectedSequence = expectedSequence;
        this.modifier = mod;
        this.context = context;
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

    public byte getSequenceNumber(){
        return header.getByte(3);
    }

    public int getNextSequenceNumber(){
        return (0xFF) & (getSequenceNumber() + 1);
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
            int codedSequence = header.getUnsignedByte(3);
            if (codedSequence != expectedSequence){
                String message = context + " , sequence problem decoding packet, expected=" + expectedSequence + " , decoded=" + codedSequence;
                logger.warn(message);
            }

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
                expectedSequence++;
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

    public static int encodeFullMessage(int sequenceStart, MysqlMessage mysql, ByteBuf destination) {

        //copies the message payload to a heap buffer, so we can size the actual output.
        ByteBuf payloadHolder = Unpooled.buffer();
        mysql.marshallPayload(payloadHolder); //copy full payload to heap buffer (might be an extended payload)

        return encodeFullPayload(sequenceStart, payloadHolder, destination);
    }

    public static int encodeFullPayload(int sequenceStart, ByteBuf payloadHolder, ByteBuf destination) {
        ByteBuf leBuf = destination.order(ByteOrder.LITTLE_ENDIAN);

        //TODO: this loop is identical to the one below, except it doesn't consume the source or update the destination.  Consolidate?
        //calculate the size of the final encoding, so we resize the destination at most once.
        int payloadRemaining = payloadHolder.readableBytes();
        int outputSize = 0;
        do {
            outputSize += 4; //header
            int chunkLength = Math.min(payloadRemaining,MAX_PAYLOAD);
            outputSize += chunkLength;
            payloadRemaining -= chunkLength;
            if (chunkLength == MAX_PAYLOAD)
                outputSize += 4; //need one more packet if last fragment is exactly 0xFFFF long.
        } while (payloadRemaining > 0);

        leBuf.ensureWritable(outputSize);

        int sequenceIter = sequenceStart;
        boolean lastChunkWasMaximumLength;
        do {
            int initialSize = payloadHolder.readableBytes();
            int maxSlice = MAX_PAYLOAD;
            int sendingPayloadSize = Math.min(maxSlice, initialSize);
            lastChunkWasMaximumLength = (sendingPayloadSize == maxSlice); //need to send a zero length payload if last fragment was exactly 0xFFFF long.

            ByteBuf nextChunk = payloadHolder.readSlice(sendingPayloadSize);
            leBuf.writeMedium(sendingPayloadSize);
            leBuf.writeByte(sequenceIter);
            leBuf.writeBytes(nextChunk);

            sequenceIter++;
        } while (payloadHolder.readableBytes() > 0 || lastChunkWasMaximumLength);
        return sequenceIter;  //returns the next usable/expected sequence number.
    }

}
