// OS_STATUS: public
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

import com.tesora.dve.common.DBType;

public abstract class PreConnectResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;
	
	protected DBType dbType;
	protected int connectID;
	protected String serverVersion;
	
	public void setDBType( DBType dbType ) {
		this.dbType = dbType;
	}
	
	public DBType getDBType() {
		return this.dbType;
	}
	
	public void setConnectId( int connectId ) {
		this.connectID = connectId;
	}
	
	public int getConnectId() {
		return this.connectID;
	}
	
	public void setServerVersion(String serverVersion) {
		this.serverVersion = serverVersion;
	}

	public String getServerVersion() {
		return this.serverVersion;
	}
}
