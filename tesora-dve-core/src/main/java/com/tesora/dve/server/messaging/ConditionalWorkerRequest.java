package com.tesora.dve.server.messaging;

import java.sql.SQLException;

import javax.transaction.xa.XAException;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;

public class ConditionalWorkerRequest extends WorkerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final WorkerRequest target;
	private final GuardFunction guard;
	
	public ConditionalWorkerRequest(WorkerRequest targ, GuardFunction guard) {
		super(targ.getContext());
		this.target = targ;
		this.guard = guard;
	}
	
	@Override
	public ResponseMessage executeRequest(Worker w,
			DBResultConsumer resultConsumer) throws SQLException, PEException,
			XAException {
		if (guard.proceed(w,resultConsumer))
			return target.executeRequest(w, resultConsumer);
		else
			return new GenericResponse();
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return target.getStatisticsNotice();
	}

	@Override
	public MessageType getMessageType() {
		return target.getMessageType();
	}

	@Override
	public MessageVersion getVersion() {
		return target.getVersion();
	}

	public interface GuardFunction {
		
		public boolean proceed(Worker w, DBResultConsumer consumer) throws PEException;
		
	}
	
}
