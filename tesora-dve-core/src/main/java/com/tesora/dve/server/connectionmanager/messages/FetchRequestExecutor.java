// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

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
