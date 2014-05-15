// OS_STATUS: public
package com.tesora.dve.server.messaging;


import java.sql.SQLException;

import javax.transaction.xa.XAException;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.worker.Worker;

public class WorkerFetchRequest extends WorkerRequest {

	private static final long serialVersionUID = 1L;

	public WorkerFetchRequest(SSContext ssContext) {
		super(ssContext);
	}

//	@Override
//	public ResponseMessage executeRequest(Worker w) throws SQLException, PEException{		
//		FetchResponse resp = new FetchResponse().from(w.getAddress());
//		boolean hasChunk = w.getChunkManager().nextChunk();
//		resp.setNoMoreData(!hasChunk);
//		if (hasChunk)
//			resp.setResultChunk(w.getChunkManager().getChunk());
//		
//		return resp;
//	}

	@Override
	public MessageType getMessageType() {
		return MessageType.W_FETCH_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}


	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return new LogSiteStatisticRequest(OperationClass.FETCH);
	}

	@Override
	public ResponseMessage executeRequest(Worker w,
			DBResultConsumer resultConsumer) throws SQLException, PEException,
			XAException {
		// TODO Auto-generated method stub
		return null;
	}
}
