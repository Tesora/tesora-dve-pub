package com.tesora.dve.groupmanager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.sql.schema.cache.qstat.HistoricalStatisticUnit;
import com.tesora.dve.sql.schema.cache.qstat.RuntimeQueryStatistic.RuntimeQueryCacheKey;

public class QueryStatisticsMessage extends GroupMessage {

	private final RuntimeQueryCacheKey key;
	private final HistoricalStatisticUnit unit;
	
	public QueryStatisticsMessage(RuntimeQueryCacheKey key, HistoricalStatisticUnit unit) {
		this.key = key;
		this.unit = unit;
	}

	public RuntimeQueryCacheKey getKey() {
		return key;
	}
	
	public HistoricalStatisticUnit getUnit() {
		return unit;
	}
	
	@Override
	void execute(HostService hostService) {
		// TODO Auto-generated method stub

	}

	@Override
	public MessageType getMessageType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		// TODO Auto-generated method stub
		return null;
	}

}
