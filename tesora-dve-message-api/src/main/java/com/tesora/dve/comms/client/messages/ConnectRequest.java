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

import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;

import com.tesora.dve.exceptions.PEException;

public class ConnectRequest extends RequestMessage {
	private static final long serialVersionUID = 1L;

	protected String userID;
	protected Object password;
	protected String project;
	protected boolean isPlain = true;
	protected String charset;
	
	public ConnectRequest() {
	}

	public ConnectRequest(String userID, String password) {
		this.userID = userID;
		this.password = password;
	}

	public String getUserID() {
		return this.userID;
	}

	public String getPassword() throws PEException {
		if ( password == null ) 
			return null;
		
		if ( password instanceof String  )
			return (String) password;
		
		if ( password instanceof byte[] )
			return new String((byte[]) password, getCharset() );
		
		throw new PEException ("Invalid object type as password in ConnectRequest " + password.getClass().getName() );
	}

	public String getProject() {
		return this.project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public Charset getCharset() {
		// if the Charset wasn't provided, default to ISO_8859_1
		if ( charset != null )
			return Charset.forName(charset);
		
		return CharsetUtil.ISO_8859_1;
	}
	
	public void setCharset(Charset charset) {
		this.charset = charset.name();
	}
	
	public boolean getIsPlaintext() {
		return isPlain;
	}

	public void setIsPlaintext(boolean isPlaintext) {
		this.isPlain = isPlaintext;
	}
	
	@Override
	public void marshallMessage() {
		if ( !getIsPlaintext()) {
			if(this.password != null && this.password instanceof String) {
				password = ((String)password).getBytes(getCharset());
			}
		}
	}
	
	@Override
	public MessageType getMessageType() {
		return MessageType.CONNECT_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
