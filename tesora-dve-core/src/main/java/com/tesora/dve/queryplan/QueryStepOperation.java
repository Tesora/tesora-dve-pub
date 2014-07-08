package com.tesora.dve.queryplan;

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

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.logutil.LogSubject;
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
import com.tesora.dve.eventing.events.QSOExecuteRequestEvent;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;

/**
 * Baseclass of <b>QueryStep</b> operations, which include, but are not limited to 
 * {@link QueryStepInsertByKeyOperation},
 * {@link QueryStepSelectAllOperation} and {@link QueryStepRedistOperation}.
 * @author mjones
 *
 */
public abstract class QueryStepOperation implements LogSubject, EventHandler {

	/**
	 * Called by <b>QueryStep</b> to execute the operation.  Any results
	 * of the operation will be returned in an instance of {@link ResultCollector}.
	 * 
	 * @param ssCon user's connection
	 * @param wg provisioned {@link WorkerGroup} against which to execute the step
	 * @param resultConsumer 
	 * @return {@link ResultCollector} with results, or <b>null</b>
	 * @throws JMSException
	 * @throws PEException
	 * @throws Throwable 
	 */
	public abstract void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable;
	
	public boolean requiresTransaction() {
		return true;
	}
	
	/**
	 * Does this query step require workers?  Default is true.  Some DDL operations are catalog-only, and
	 * thus do not require workers.
	 */
	public boolean requiresWorkers() {
		return true;
	}
	
	/**
	 * Returns true if the QueryStepOperation causes any inflight transactions to be committed
	 * first.
	 */
	public boolean requiresImplicitCommit() {
		return false;
	}	

	@Override
	public String describeForLog() {
		return this.getClass().getSimpleName();
	}
	
	public PersistentDatabase getContextDatabase() {
		return null;
	}
	
	@Override
	public Response request(Request in) {
		return sm.request(in);
	}

	@Override
	public void response(Response in) {
		sm.response(in);
	}

	protected State getImplState(EventStateMachine esm, AbstractEvent event) {
		return new QSOExecutor();
	}
	
	// right now we're just going to wrap it in a threaded execute call
	class QSOExecutor extends TransitionReqRespState {

		@Override
		protected ChildRunnable buildRunnable(EventStateMachine ems, Request req) {
			final QSOExecuteRequestEvent reqEvent = (QSOExecuteRequestEvent) req;
			return new ChildRunnable() {

				@Override
				public ResponseMessage run() throws Throwable {
					// TODO Auto-generated method stub
					execute(reqEvent.getConnection(),reqEvent.getWorkerGroup(),reqEvent.getConsumer());
					return null;
				}
				
			};
		}

		@Override
		public String getName() {
			return "QSOExecutor";
		}
		
	}
	
	private final State DISPATCHER = new DispatchState("QSODispatch") {
		
		public State getTarget(EventStateMachine esm, AbstractEvent event) {
			return getImplState(esm,event);
		}
	};
	
	private final EventStateMachine sm = new EventStateMachine("QSO",DISPATCHER);
}