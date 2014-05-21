// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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

import com.tesora.dve.exceptions.PEException;

public class MyReplSlaveState {

	MyReplicationSlaveService plugin;
	MyReplSlaveClientConnection myClient;
	
	public MyReplSlaveState(MyReplicationSlaveService plugin, 
			MyReplSlaveClientConnection myClient) throws PEException {
		this.plugin = plugin;
		this.myClient = myClient;
	}

	public void handShake() throws PEException {
		if (!myClient.start(new MyServerHandshakeHandler(plugin, myClient))) {
			throw new PEException("Could not connect to master at " + plugin.getMasterLocator());
		}
	}
	
}
