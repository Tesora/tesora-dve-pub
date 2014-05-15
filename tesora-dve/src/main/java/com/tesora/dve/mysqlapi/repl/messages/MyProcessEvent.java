// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public interface MyProcessEvent {

	public abstract void processEvent(MyReplicationSlaveService plugin) throws PEException;

	public abstract String getSkipErrorMessage();
}
