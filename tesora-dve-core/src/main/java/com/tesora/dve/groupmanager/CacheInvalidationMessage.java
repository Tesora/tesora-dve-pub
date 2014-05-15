// OS_STATUS: public
package com.tesora.dve.groupmanager;


import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;

public class CacheInvalidationMessage extends GroupMessage {

	private final CacheInvalidationRecord theRecord;
	
	public CacheInvalidationMessage(CacheInvalidationRecord cir) {
		theRecord = cir;
	}
	
	private static final long serialVersionUID = 1L;
	
	@Override
	void execute(HostService hostService) {
		if (theRecord.getGlobalToken() != null) 
			RandomDistributionModel.clearCache();
		SchemaSourceFactory.onModification(theRecord);
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
