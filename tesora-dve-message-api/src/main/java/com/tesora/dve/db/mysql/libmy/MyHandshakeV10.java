// OS_STATUS: public
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

import static com.tesora.dve.db.mysql.libmy.MyProtocolDefs.MYSQL_PROTOCOL_VERSION;
import static com.tesora.dve.db.mysql.libmy.MyProtocolDefs.SERVER_STATUS_AUTOCOMMIT;

import com.tesora.dve.db.mysql.common.JavaCharsetCatalog;
import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.db.mysql.common.MysqlHandshake;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;

public class MyHandshakeV10 extends MyResponseMessage {
	private byte protocolVersion = MYSQL_PROTOCOL_VERSION;
	private String serverVersion;
	private int threadID;
	private String salt;  // the full SALT which get broken up into the 2 scramble buffers
	private String scrambleBuffer1st;
	private String scrambleBuffer2nd;
	private Integer scrambleBufferSize;
	private byte[] serverCapabilities;
	private long serverCapabilitiesasLong;
	private byte serverCharset;
	private short serverStatus = SERVER_STATUS_AUTOCOMMIT;
	private String plugInProvidedData;

	public MyHandshakeV10() {
		setPacketNumber((byte) 0); // ServerGreeting is always packet # 0
	};
	
	public MyHandshakeV10(int connectionId, MysqlHandshake handshake) {
		setThreadID(connectionId);
		setServerVersion(handshake.getServerVersion());
		setSalt(handshake.getSalt());
		setServerCapabilities(handshake.getServerCapabilities());
		setServerCharset(handshake.getServerCharSet());
		setPlugInProvidedData(handshake.getPluginData());
	}

	public void setSalt(String salt) {
		this.salt = salt;
		scrambleBuffer1st = salt.substring(0,8);
		scrambleBuffer2nd = salt.substring(8) + '\0';
		scrambleBufferSize = scrambleBuffer1st.length() + scrambleBuffer2nd.length();
	}
	
	public String getSalt() {
		if (salt == null) {
			salt = scrambleBuffer1st + scrambleBuffer2nd;
		}
		return salt;
	}

	byte getServerCapabilities(short byteNum) {
		return serverCapabilities[byteNum];
	}
	
	public long getServerCapabilities() {
		return serverCapabilitiesasLong;
	}

	public void setServerCapabilities(long sc) {
		serverCapabilitiesasLong = sc;
		// store the capabilities flag in a byte[] to facilitate retrieval by
		// byte later
		serverCapabilities = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(sc).array();
	}
	
	public String getServerVersion() {
		return serverVersion;
	}

	public void setServerVersion(String serverVersion) {
		this.serverVersion = serverVersion;
	}

	public byte getServerCharsetId() {
		return serverCharset;
	}

	public void setServerCharset(byte serverCharset) {
		this.serverCharset = serverCharset;
	}

	public String getPlugInProvidedData() {
		return plugInProvidedData;
	}

	public void setPlugInProvidedData(String plugInProvidedData) {
		this.plugInProvidedData = plugInProvidedData;
	}

	public int getThreadID() {
		return threadID;
	}

	public void setThreadID(int threadID) {
		this.threadID = threadID;
	}

	@Override
	public void marshallMessage(ByteBuf cb) {
		cb.writeByte(protocolVersion);
		cb.writeBytes(getServerVersion().getBytes());
		cb.writeZero(1);
		cb.writeInt(getThreadID());
		cb.writeBytes(scrambleBuffer1st.getBytes()); // Salt
		cb.writeZero(1);
		cb.writeByte(getServerCapabilities((byte) 0));
		cb.writeByte(getServerCapabilities((byte) 1));
		cb.writeByte(getServerCharsetId());
		cb.writeShort(serverStatus);
		cb.writeByte(getServerCapabilities((byte) 2));
		cb.writeByte(getServerCapabilities((byte) 3));
		cb.writeByte(scrambleBufferSize.byteValue());
		cb.writeZero(10); // write 10 unused bytes
		cb.writeBytes(scrambleBuffer2nd.getBytes()); // Salt
		cb.writeBytes(getPlugInProvidedData().getBytes()); // payload
		cb.writeZero(1);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		protocolVersion = cb.readByte();
		serverVersion = cb.readSlice(cb.bytesBefore((byte) 0)).toString(CharsetUtil.UTF_8);
		cb.skipBytes(1); // skip the NULL terminator
		threadID = cb.readInt();
		scrambleBuffer1st = MysqlAPIUtils.readBytesAsString(cb, 8, CharsetUtil.ISO_8859_1);
		cb.skipBytes(1); 
		long sc1 = cb.readUnsignedShort();
		serverCharset = cb.readByte();
		serverStatus = cb.readShort();
		long sc2 = Long.rotateLeft(cb.readUnsignedShort(),16);
		setServerCapabilities(sc1 + sc2);
		scrambleBufferSize = new Integer(cb.readByte());
		cb.skipBytes(10); //unused bytes
		scrambleBuffer2nd = cb.readSlice(cb.bytesBefore((byte) 0)).toString(CharsetUtil.ISO_8859_1);
		cb.skipBytes(1); 
		if ( (serverCapabilitiesasLong & ClientCapabilities.CLIENT_PLUGIN_AUTH) == ClientCapabilities.CLIENT_PLUGIN_AUTH) {
			plugInProvidedData = cb.readSlice(cb.bytesBefore((byte) 0)).toString(CharsetUtil.UTF_8);
		}
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.SERVER_GREETING_RESPONSE;
	}

	public Charset getServerCharset(JavaCharsetCatalog catalog) {
		return catalog.findJavaCharsetById(serverCharset);
	}


}
