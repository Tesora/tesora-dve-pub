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

import com.tesora.dve.db.mysql.MysqlNativeConstants;

public class Packet {

	public static final class ExtendedPacket extends Packet {

		protected ExtendedPacket(final int initialCapacity) {
			super(initialCapacity);
		}

		protected ExtendedPacket(final byte messageType, final int initialCapacity) {
			super(messageType, initialCapacity);
		}

		@Override
		public boolean isExtended() {
			return true;
		}
	}

	private static final Logger logger = Logger.getLogger(Packet.class);

	private static final byte DEFAULT_SEQUENCE_ID = 0;
	private static final byte DEFAULT_MESSAGE_TYPE = -1;

	private byte sequenceId;
	private byte messageType;
	private ByteBuf payloadBuffer;

	public static Packet buildPacket(final int payloadLength) {
		if (payloadLength < MysqlNativeConstants.MAX_PAYLOAD_SIZE) {
			return new Packet(payloadLength);
		}

		return new ExtendedPacket(payloadLength);
	}

	protected Packet(final int initialCapacity) {
		this(DEFAULT_SEQUENCE_ID, DEFAULT_MESSAGE_TYPE, initialCapacity, ByteOrder.LITTLE_ENDIAN);
	}

	protected Packet(final byte messageType, final int initialCapacity) {
		this(DEFAULT_SEQUENCE_ID, messageType, initialCapacity, ByteOrder.LITTLE_ENDIAN);
	}

	protected Packet(final byte sequenceId, final byte messageType, final int initialCapacity, final ByteOrder order) {
		this.sequenceId = sequenceId;
		this.messageType = messageType;
		this.initializePayloadBuffer(initialCapacity, order);
	}

	public final void writePacketPayload(final ByteBuf frame, final int payloadLength, final byte sequenceId) {
		if (logger.isDebugEnabled()) {
			final int currentWriterIndex = this.payloadBuffer.writerIndex();
			logger.debug("Writing payload into '" + this.toString() + "': id=" + sequenceId + "; writerIndex=" + currentWriterIndex + "; length="
					+ payloadLength);
		}
		this.sequenceId = sequenceId;
		this.payloadBuffer.writeBytes(frame, payloadLength);
	}

	public final byte getSequenceId() {
		return this.sequenceId;
	}

	public final void setSequenceId(final byte sequenceId) {
		this.sequenceId = sequenceId;
	}

	public final byte getMessageType() {
		return this.messageType;
	}

	public final void setMessageType(final byte messageType) {
		this.messageType = messageType;
	}

	/**
	 * The buffer is not managed by this class past allocation.
	 * The caller is responsible for further memory management and cleanup.
	 */
	public final ByteBuf getPayload() {
		return this.payloadBuffer;
	}

	public boolean isExtended() {
		return false;
	}

	@Override
	public String toString() {
		final Class<?> clazz = this.getClass();
		return clazz.getSimpleName() + " (" + Integer.toHexString(this.hashCode()) + ")";
	}

	private void initializePayloadBuffer(final int initialCapacity, final ByteOrder order) {
		this.payloadBuffer = Unpooled.buffer(initialCapacity + 1).order(order);
	}
}