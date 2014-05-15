// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;

public class SSConnectionAccessor {
	
	static public SSConnection getSSConnection(SSConnectionProxy proxy) {
		return proxy.ssConnection;
	}

	static public void callDoPostReplyProcessing(SSConnection ssCon) throws PEException {
		ssCon.doPostReplyProcessing();
	}
	
	static public void setCatalogDAO(SSConnection ssCon, CatalogDAO c) {
		ssCon.txnCatalogDAO = c;
	}
}
