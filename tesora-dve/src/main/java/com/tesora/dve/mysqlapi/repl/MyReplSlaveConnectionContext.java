// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

import com.tesora.dve.mysqlapi.MyClientConnectionContext;

public class MyReplSlaveConnectionContext extends MyClientConnectionContext {

	public MyReplSlaveConnectionContext(String connectHost, int connectPort) {
		super(connectHost, connectPort);
	}

}
