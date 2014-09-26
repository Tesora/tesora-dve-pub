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


import com.tesora.dve.db.mysql.MysqlMessage;
import com.tesora.dve.db.mysql.portal.protocol.Packet;
import io.netty.buffer.ByteBuf;

import java.nio.ByteOrder;

public abstract class MyMessage implements MysqlMessage, MyMarshallMessage, MyUnmarshallMessage {
	public static final short MESSAGE_HEADER_LENGTH = 4;
    boolean endsSequence = false;
	// ---------
	// classes extending MyMessage should implement this methods to
	// return the relevant value.
    // NOTE: these values are the identifiers used in replication.
	public abstract MyMessageType getMessageType();
	// -------


    public void marshallPayload(ByteBuf destination) {
        destination = destination.order(ByteOrder.LITTLE_ENDIAN);

        if ( this.isMessageTypeEncoded() )
            destination.writeByte(this.getMessageType().getByteValue());

        this.marshallMessage(destination);
    }

    public boolean isSequenceEnd() {
        return endsSequence;
    }

    public void setSequenceEnd(boolean endsSequence) {
        this.endsSequence = endsSequence;
    }

    @Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
