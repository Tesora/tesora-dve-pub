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
import io.netty.buffer.ByteBuf;

import java.nio.ByteOrder;

public abstract class MyMessage implements MyMarshallMessage, MyUnmarshallMessage {
	public static final short MESSAGE_HEADER_LENGTH = 4;
	
	private byte packetNumber;

	// ---------
	// classes extending MyMessage should implement this methods to
	// return the relevant value.
	public abstract MyMessageType getMessageType();
	// -------

	public byte getPacketNumber() {
		return packetNumber;
	}

	public void setPacketNumber(byte packetNumber) {
		this.packetNumber = packetNumber;
	}

	public void setPacketNumber(int packetNumber) {
		setPacketNumber((byte) packetNumber);
	}
	
	public MyMessage withPacketNumber(int packetNumber) {
		setPacketNumber((byte) packetNumber);
		return this;
	}

    public int marshalZeroHeader(ByteBuf destination){
        int startIndex = destination.writerIndex();
        destination.writeZero(MESSAGE_HEADER_LENGTH);
        return startIndex;
    }

    public void updateHeader(ByteBuf destination, int offset, int length, byte sequence){
        destination = destination.order(ByteOrder.LITTLE_ENDIAN);
        destination.setMedium(offset,length);
        destination.setByte(offset + 3,sequence);
    }

    public void marshallPayload(ByteBuf destination) throws PEException {
        destination = destination.order(ByteOrder.LITTLE_ENDIAN);

        if ( this.isMessageTypeEncoded() )
            destination.writeByte(this.getMessageType().getByteValue());

        this.marshallMessage(destination);
    }

    public void marshallFullMessage(ByteBuf destination) {
        int startIndex = this.marshalZeroHeader(destination);
        try {
            this.marshallPayload(destination);
        } catch (PEException e) {
            throw new PECodingException(e);
        }
        int size = destination.writerIndex() - startIndex - MyMessage.MESSAGE_HEADER_LENGTH;
        this.updateHeader(destination, startIndex, size, this.getPacketNumber());
    }
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + " packetNumber=" + packetNumber;
	}
}
