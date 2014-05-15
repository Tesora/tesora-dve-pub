// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;


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
