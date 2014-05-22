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

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;

public class MyLoginRequest extends MyMessage {

	private long clientCapabilities;
	private int maxPacketSize;
	private byte clientCharset;
	protected String username;
	protected String password;
	protected String database;
	private String plugInData;

	public MyLoginRequest() {
		super();
	}
	
	public MyLoginRequest(String username, String password) {
		this.username = username;
		this.password = password;
	}

	@Override
	public boolean isMessageTypeEncoded() {
		// Login Request message type isn't included encoded in the wire format
		return false;
	}

	public long getClientCapabilities() {
		return clientCapabilities;
	}

	public MyLoginRequest setClientCapabilities(long clientCapabilities) {
		this.clientCapabilities = clientCapabilities;
		return this;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public MyLoginRequest setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
		return this;
	}

	public byte getClientCharset() {
		return clientCharset;
	}

	public MyLoginRequest setClientCharset(byte clientCharset) {
		this.clientCharset = clientCharset;
		return this;
	}

	public String getUsername() {
		return username;
	}

	public MyLoginRequest setUsername(String username) {
		this.username = username;
		return this;
	}

	public String getPassword() {
		return password;
	}

	public MyLoginRequest setPassword(String password) {
		this.password = password;
		return this;
	}

	public String getDatabase() {
		return database;
	}

	public MyLoginRequest setDatabase(String database) {
		this.database = database;
		return this;
	}

	public String getPlugInData() {
		return plugInData;
	}

	public MyLoginRequest setPlugInData(String plugInData) {
		this.plugInData = plugInData;
		return this;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		clientCapabilities = cb.readUnsignedInt();
		boolean hasConnectDatabase = ((clientCapabilities & ClientCapabilities.CLIENT_CONNECT_WITH_DB) == 
				ClientCapabilities.CLIENT_CONNECT_WITH_DB);
		maxPacketSize = cb.readInt();
		clientCharset = cb.readByte();
	
		cb.skipBytes(23); // login request has a 23 byte filler
		username = cb.readSlice(cb.bytesBefore((byte) 0)).toString(CharsetUtil.UTF_8);
		cb.skipBytes(1); // skip the NULL terminator
		byte passwordLength = cb.readByte();
		byte[] passwordBytes = new byte[passwordLength];
		cb.getBytes(cb.readerIndex(), passwordBytes, 0, passwordLength);
		password = new String(passwordBytes, CharsetUtil.ISO_8859_1);
		cb.skipBytes(passwordLength);
		// if the clientCapabilities flag has the CLIENT_CONNECT_WITH_DB bit set,
		// then this message contains an initial database to connect to
		if ( hasConnectDatabase ) {
			database = cb.readSlice(cb.bytesBefore((byte) 0)).toString(CharsetUtil.UTF_8);
			if (database.length() < 1) {
				database = null;
			}
		}
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		boolean hasConnectDatabase = false;
		if ( database != null ) {
			clientCapabilities = clientCapabilities + ClientCapabilities.CLIENT_CONNECT_WITH_DB;
			hasConnectDatabase = true;
		}
		cb.writeInt((int)clientCapabilities);
		cb.writeInt(maxPacketSize);
		cb.writeByte(clientCharset);
		cb.writeZero(23);	// filler
		cb.writeBytes(username.getBytes(CharsetUtil.UTF_8));
		cb.writeZero(1);	// null terminator for username
		byte[] passwordBytes = password.getBytes(CharsetUtil.ISO_8859_1);
		MysqlAPIUtils.putLengthCodedString(cb, passwordBytes, false);
		if ( hasConnectDatabase ) { 
			cb.writeBytes(database.getBytes(CharsetUtil.UTF_8));
			cb.writeZero(1);	// null terminator for database
		}
		if ( plugInData != null ) {
			cb.writeBytes(plugInData.getBytes(CharsetUtil.UTF_8));
			cb.writeZero(1);	// null terminator for plugInData
		}
		setPacketNumber(1);
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.LOGIN_REQUEST;
	}
	
	@Override
	public String toString() {
		return super.toString() + " username=" + username + " database=" + database;
	}


}