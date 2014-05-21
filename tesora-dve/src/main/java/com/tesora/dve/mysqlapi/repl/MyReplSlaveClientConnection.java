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
import com.tesora.dve.mysqlapi.MyClientConnection;
import com.tesora.dve.mysqlapi.MyClientSideAsyncDecoder;

public class MyReplSlaveClientConnection extends MyClientConnection {

	private MyReplicationSlaveService plugin;

	public MyReplSlaveClientConnection(MyReplSlaveConnectionContext rsContext,
			MyReplicationSlaveService plugin) {
		super(rsContext);
		this.plugin = plugin;
	}

	public MyReplicationSlaveService getMyReplicationSlavePlugin() {
		return plugin;
	}

	public void useAsynchMode() throws PEException {
		changeToAsyncPipeline(new MyClientSideAsyncDecoder(),
				new MyReplSlaveAsyncHandler(getContext(), plugin));
	}
}
