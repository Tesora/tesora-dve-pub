// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.server.global.HostService;
import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;

public class SiteFailureMessage extends GroupMessage {

	private static final long serialVersionUID = 1L;
	
	static Logger logger = Logger.getLogger(SiteFailureMessage.class);

	int siteInstanceId;

	public int getSiteInstanceId() {
		return siteInstanceId;
	}

	public SiteFailureMessage(int siteInstanceId) {
		this.siteInstanceId = siteInstanceId;
	}

	@Override
	void execute(HostService hostService) {
		try {
			PerHostConnectionManager.INSTANCE.sendToAllConnections(this);
			SchemaSourceFactory.onModification(CacheInvalidationRecord.GLOBAL);
		} catch (PEException e) {
			logger.warn("Unable to execute site failover for site instance " + siteInstanceId, e);
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(" + siteInstanceId + ")";
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		return null;
	}

}
