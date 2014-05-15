// OS_STATUS: public
package com.tesora.dve.server.messaging;

import java.sql.SQLException;

import javax.transaction.xa.XAException;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.comms.client.messages.RequestMessage;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;

/**
 * WorkerRequest is the base class of any requests to be executed in the context of a worker.  Since the message 
 * will be executed on several workers in parallel, any member variables of classes which derive from WorkerRequest
 * should be final (otherwise, one worker could change the parameters seen by another worker, which would 
 * cause concurrency issues).
 *  
 */
@SuppressWarnings("serial")
public abstract class WorkerRequest extends RequestMessage {
	
	final SSContext connectionContext;
	
	boolean autoTransact = true;
	
	public WorkerRequest(SSContext ctx) {
		connectionContext = ctx;
	}
	
	public boolean isAutoTransact() {
		return autoTransact && !StringUtils.isEmpty(connectionContext.getTransId());
	}

	public void setAutoTransact(boolean autoTransact) {
		this.autoTransact = autoTransact;
	}
	
	public WorkerRequest forDDL() {
		autoTransact = false;
		return this;
	}

	public SSContext getContext() {
		return connectionContext;
	}
	
	public String getTransId() {
		return connectionContext.getTransId();
	}

	public abstract ResponseMessage executeRequest(Worker w, DBResultConsumer resultConsumer) throws SQLException, PEException, XAException;
	
	@Override
	public String toString() {
		return new StringBuffer(getClass()
				.getSimpleName()+"{type=").append(getMessageType())
				.append(", globalId=").append(connectionContext.getTransId())
				.append(", autoTrans=").append(autoTransact).append("}"
						).toString();
	}
	
	public int getConnectionId() {
		return connectionContext.getConnectionId();
	}

	public abstract LogSiteStatisticRequest getStatisticsNotice();
}
