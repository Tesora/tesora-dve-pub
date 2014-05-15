// OS_STATUS: public
package com.tesora.dve.server.messaging;

import java.sql.SQLException;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;

public class ResetWorkerRequest extends WorkerRequest {

	public ResetWorkerRequest(SSContext ctx) {
		super(ctx);
	}

	private static final long serialVersionUID = 1L;
	
	@Override
	public ResponseMessage executeRequest(Worker w, DBResultConsumer resultConsumer) throws SQLException {
		w.resetStatement();
		return new GenericResponse().success();
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.RESET_WORKER;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}


	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return null;
	}
}
