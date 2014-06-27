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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.eventing.AbstractEvent;
import com.tesora.dve.eventing.AbstractReqRespState;
import com.tesora.dve.eventing.EventHandler;
import com.tesora.dve.eventing.EventStateMachine;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.eventing.Response;
import com.tesora.dve.eventing.StackAction;
import com.tesora.dve.eventing.State;
import com.tesora.dve.eventing.TransitionResult;
import com.tesora.dve.eventing.events.ExceptionResponse;
import com.tesora.dve.eventing.events.QSOExecuteRequestEvent;
import com.tesora.dve.eventing.events.WorkerGroupSubmitEvent;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation.QSOExecutor;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.worker.MysqlParallelResultConsumer;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

/**
 * {@link QueryStepSelectAllOperation} is a <b>QueryStep</b> operation which
 * executes a sql query against a {@link PersistentGroup}, returning the results
 * of the query.
 *
 */
@XmlType(name="QueryStepSelectAllOperation")
public class QueryStepSelectAllOperation extends QueryStepResultsOperation {

	static Logger logger = Logger.getLogger( QueryStepSelectAllOperation.class );
	
	@XmlElement(name="DistributionModel")
	DistributionModel distributionModel;
	@XmlElement(name="SqlCommand")
	SQLCommand command;
	
	/**
	 * Constructs a <b>QueryStepSelectAllOperation</b> to execute the provided 
	 * sql query against the database specified by <em>execCtxDBName</em>.
	 * 
	 * @param execCtxDBName database in which the <em>command</em> will be executed
	 * @param command sql query to execute
	 * @throws PEException 
	 */
	public QueryStepSelectAllOperation(PersistentDatabase execCtxDBName, DistributionModel distModel, SQLCommand command) throws PEException {
		super(execCtxDBName);
		if (command.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		this.distributionModel= distModel;
		this.command = command;
	}

	public QueryStepSelectAllOperation(PersistentDatabase execCtxDBName, DistributionModel distModel, String command) throws PEException {
		this(execCtxDBName, distModel, new SQLCommand(command));
	}

		/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		resultConsumer.setResultsLimit(getResultsLimit());
		WorkerExecuteRequest req = new WorkerExecuteRequest(ssCon.getTransactionalContext(), command).onDatabase(database);
		beginExecution();
		wg.execute(distributionModel.mapForQuery(wg, command), req, resultConsumer);
		if (resultConsumer instanceof MysqlParallelResultConsumer) 
			endExecution(((MysqlParallelResultConsumer)resultConsumer).getNumRowsAffected());
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}
	
	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		buf.append("SelectAll stmt=").append(command.getRawSQL());
		return buf.toString();
	}

	@Override
	protected State getImplState(EventStateMachine esm, AbstractEvent event) {
		return new SelectAllSM();
	}

	class SelectAllSM extends AbstractReqRespState {

		QSOExecuteRequestEvent request;
		
		@Override
		public TransitionResult onEvent(EventStateMachine esm,
				AbstractEvent event) {
			if (event.isRequest()) {
				request = (QSOExecuteRequestEvent) event;
				try {
					request.getConsumer().setResultsLimit(getResultsLimit());
					MappingSolution ms = distributionModel.mapForQuery(request.getWorkerGroup(), command);
					WorkerExecuteRequest req = new WorkerExecuteRequest(request.getConnection().getTransactionalContext(), command).onDatabase(database);
					WorkerGroupSubmitEvent submitEvent = 
							new WorkerGroupSubmitEvent(esm,ms,req,request.getConsumer(),0);
					return new TransitionResult()
					.withTargetEvent(submitEvent, request.getWorkerGroup());
				} catch (PEException pe) {
					return propagateRequestException(request,pe);
				}
			} else {
				return propagateResponse(esm,request,event);
			}
		}
		
	}
	
}
