package com.tesora.dve.server.messaging;

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

import java.sql.SQLException;

import javax.transaction.xa.XAException;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.comms.client.messages.RequestMessage;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.eventing.AbstractEvent;
import com.tesora.dve.eventing.DispatchState;
import com.tesora.dve.eventing.EventHandler;
import com.tesora.dve.eventing.EventStateMachine;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.eventing.Response;
import com.tesora.dve.eventing.State;
import com.tesora.dve.eventing.TransitionReqRespState;
import com.tesora.dve.eventing.events.WorkerExecuteRequestEvent;
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
public abstract class WorkerRequest extends RequestMessage implements EventHandler {
	
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
	
	@Override
	public Response request(Request in) {
		return sm.request(in);
	}

	@Override
	public void response(Response in) {
		sm.response(in);
	}

	protected State getImplState(EventStateMachine esm, AbstractEvent event) {
		return new WorkerRequestExecutor();
	}
	
	public class WorkerRequestExecutor extends TransitionReqRespState {

		@Override
		protected ChildRunnable buildRunnable(EventStateMachine ems, Request req) {
			final WorkerExecuteRequestEvent wreq = (WorkerExecuteRequestEvent) req;
			return new ChildRunnable() {

				@Override
				public ResponseMessage run() throws Throwable {
					return executeRequest(wreq.getWorker(),wreq.getConsumer());
				}
				
			};
		}

		@Override
		public String getName() {
			return "WorkerRequestExecutor";
		}
		
	}
	
	private final State DISPATCHER = new DispatchState("WorkerRequest.DISPATCHER") {
		
		public State getTarget(EventStateMachine esm, AbstractEvent event) {
			return getImplState(esm,event);
		}
	};
	
	private final EventStateMachine sm = new EventStateMachine(getClass().getSimpleName(),DISPATCHER);
}
