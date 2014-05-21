// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
