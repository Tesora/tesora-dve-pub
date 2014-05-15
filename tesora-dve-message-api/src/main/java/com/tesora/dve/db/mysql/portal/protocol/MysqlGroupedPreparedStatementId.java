// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PECodingException;

public class MysqlGroupedPreparedStatementId implements
		GroupedPreparedStatementId {
	
	private static final Logger logger = Logger.getLogger(MysqlGroupedPreparedStatementId.class);

	Map<Channel, Integer> stmtIdMap = new ConcurrentHashMap<Channel, Integer>();

	public void addStmtId(Channel channel, int stmtId) {
		if (logger.isDebugEnabled())
			logger.debug("Statement id " + stmtId + " registered for channel " + channel);
		stmtIdMap.put(channel, stmtId);
	}

	public int getStmtId(Channel channel) {
		if (!stmtIdMap.containsKey(channel))
			throw new PECodingException("No prepared statement id registered for channel " + channel + "idMap: " + this);
		return stmtIdMap.get(channel);
	}

	@Override
	public String toString() {
		return super.toString() + "{" + stmtIdMap + "}";
	}
}
