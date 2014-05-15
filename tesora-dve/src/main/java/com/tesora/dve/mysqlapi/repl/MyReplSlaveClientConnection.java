// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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
