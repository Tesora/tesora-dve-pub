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
import com.tesora.dve.server.connectionmanager.SSConnection;

public class FetchRequestExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection agent, Object message)
			throws Throwable {
		// TODO Auto-generated method stub
		return null;
	}

//	@Override
//	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
//		FetchRequest fr = (FetchRequest) message;
//		FetchResponse resp = new FetchResponse();
//
//		SSStatement stmt = connMgr.getStatementForResults(fr.getStatementId());
//		ResultCollector rc = stmt.getQueryPlan().getResultCollector();
//
//		if (rc != null) {
//			boolean noMoreData = true;
//			if (rc != null) {
//				noMoreData = rc.noMoreData();
//				if (noMoreData)
//					connMgr.clearStatementResources(stmt);
//				else
//					resp.setResultChunk(rc.getChunk());
//			}
//			resp.setNoMoreData(noMoreData);
//		} else {
//			resp.setNoMoreData();
//		}
//		return resp;
//	}

}
