package com.tesora.dve.comms.client.messages;

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

public class MysqlPreConnectResponse extends PreConnectResponse {

	private static final long serialVersionUID = 1L;

	protected String salt;
	protected int charset;
	protected String pluginData;
	protected long serverCapabilities;
	
	public void setSalt( String salt ) {
		this.salt = salt;
	}
	
	public String getSalt() {
		return this.salt;
	}

	public void setCharset( int charset ) {
		this.charset = charset;
	}
	
	public int getCharset() {
		return this.charset;
	}

	public void setServerCapabilities( long serverCap ) {
		this.serverCapabilities = serverCap;
	}
	
	public long getServerCapabilities() {
		return serverCapabilities;
	}

	public void setPluginData( String pluginData ) {
		this.pluginData = pluginData;
	}
	
	public String getPluginData() {
		return this.pluginData;
	}
	
	@Override
	public MessageType getMessageType() {
		return MessageType.MYSQL_PRECONNECT_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}
}
