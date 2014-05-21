// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

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

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.groupmanager.PurgeWorkerGroupCaches;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class PurgeWorkerGoupCacheExecutor implements AgentExecutor<SSConnection> {
	
	public final static PurgeWorkerGoupCacheExecutor INSTANCE = new PurgeWorkerGoupCacheExecutor();

	@Override
	public ResponseMessage execute(SSConnection ssCon, Object message) throws Throwable {
		PurgeWorkerGroupCaches purgeRequest = (PurgeWorkerGroupCaches) message;
		ssCon.clearWorkerGroupCache(purgeRequest.getGroup());
		return null;
	}

}
