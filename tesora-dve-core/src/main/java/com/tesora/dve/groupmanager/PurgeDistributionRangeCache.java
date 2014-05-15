// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.server.global.HostService;

public class PurgeDistributionRangeCache extends GroupMessage {

	private static final long serialVersionUID = 1L;
	
	int distributionRangeId;

	public PurgeDistributionRangeCache(int distributionRangeId) {
		this.distributionRangeId = distributionRangeId;
	}

	@Override
	void execute(HostService hostService) {
		RangeDistributionModel.clearCacheEntry(distributionRangeId);
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
