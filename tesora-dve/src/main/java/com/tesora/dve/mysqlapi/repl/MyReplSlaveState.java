// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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
