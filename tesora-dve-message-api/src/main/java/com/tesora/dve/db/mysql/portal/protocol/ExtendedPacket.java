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
import io.netty.buffer.Unpooled;

import java.nio.ByteOrder;

import org.apache.log4j.Logger;

public final class ExtendedPacket {

	private static final Logger logger = Logger.getLogger(ExtendedPacket.class);

	private byte sequenceId;
	private byte messageType;
	private ByteBuf payloadBuffer;

	public ExtendedPacket(final byte messageType, final int initialCapacity) {
		this.messageType = messageType;
		this.payloadBuffer = Unpooled.buffer(initialCapacity + 1).order(ByteOrder.LITTLE_ENDIAN);
	}

	public void writePacketPayload(final ByteBuf frame, final int payloadLength, final byte sequenceId) {
		if (logger.isDebugEnabled()) {
			logger.debug("Reading an extended packet: id=" + sequenceId + "; payload=" + payloadLength);
		}
		this.sequenceId = sequenceId;
		this.payloadBuffer.writeBytes(frame, payloadLength);
	}

	public byte getSequenceId() {
		return this.sequenceId;
	}

	public byte getMessageType() {
		return this.messageType;
	}

	public ByteBuf getPayload() {
		return this.payloadBuffer;
	}

	public boolean releasePayload() {
		if ((this.payloadBuffer != null) && this.payloadBuffer.release()) {
			this.payloadBuffer = null;
			return true;
		}

		return false;
	}
}