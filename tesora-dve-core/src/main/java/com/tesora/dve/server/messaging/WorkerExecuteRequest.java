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

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.eventing.AbstractEvent;
import com.tesora.dve.eventing.EventStateMachine;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.eventing.Response;
import com.tesora.dve.eventing.SequentialExecutionMode;
import com.tesora.dve.eventing.SequentialState;
import com.tesora.dve.eventing.SequentialStep;
import com.tesora.dve.eventing.SequentialTransition;
import com.tesora.dve.eventing.StackAction;
import com.tesora.dve.eventing.State;
import com.tesora.dve.eventing.events.AcknowledgeResponse;
import com.tesora.dve.eventing.events.WorkerExecuteEvent;
import com.tesora.dve.eventing.events.WorkerExecuteRequestEvent;
import com.tesora.dve.eventing.events.WrappedResponseMessageEvent;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerStatement;

public class WorkerExecuteRequest extends WorkerRequest {
	
	static Logger logger = Logger.getLogger( WorkerExecuteRequest.class );

	private static final long serialVersionUID = 1L;

	PersistentDatabase defaultDatabase;
	final SQLCommand command;

	boolean recoverLocks;

	public WorkerExecuteRequest(SSContext ssContext, SQLCommand command) {
		super(ssContext);
		this.command = command;
	}
	
	public WorkerExecuteRequest onDatabase(PersistentDatabase database) {
		defaultDatabase = database;
		return this;
	}

	public WorkerExecuteRequest withLockRecovery(boolean recoverLocks) {
		this.recoverLocks = recoverLocks;
		return this;
	}
	
	@Override
	public ResponseMessage executeRequest(Worker w, DBResultConsumer resultConsumer) throws SQLException, XAException, PEException {
		return executeStatement(w, getCommand(), resultConsumer);
	}
	
	protected ResponseMessage executeStatement(Worker w, SQLCommand stmtCommand, DBResultConsumer resultConsumer) throws SQLException, PEException, XAException {
		ResponseMessage resp = null;
		long rowCount = -1;
		boolean hasResults = false;
		ColumnSet rsmd = null;
		Exception anyException = null;
				
		w.setCurrentDatabase(defaultDatabase);
		
		// do any late resolution
		
		if (isAutoTransact())
			w.startTrans(getTransId());
		
		try {
			String savepointId = null;
			
			if (recoverLocks) {
				savepointId = "barrier" + w.getUniqueValue();
				w.getStatement().execute(getConnectionId(), new SQLCommand("savepoint " + savepointId),
						DBEmptyTextResultConsumer.INSTANCE);
			}

			WorkerStatement stmt;
//			if (stmtCommand.isPreparedStatement()) {
//				WorkerPreparedStatement pstmt = w.prepareStatement(stmtCommand);
//				stmtCommand.fillParameters(pstmt);
//				hasResults = pstmt.execute();
//				stmt = pstmt;
//			} else {
				stmt = w.getStatement();
				hasResults = stmt.execute(getConnectionId(), stmtCommand, resultConsumer);
//			}
			
			if (recoverLocks) {
				boolean rowsFound = (hasResults && stmt.getResultSet().isBeforeFirst()) 
						|| (!hasResults && resultConsumer.getUpdateCount() > 0);
				if (!rowsFound) {
					w.getStatement().execute(getConnectionId(), new SQLCommand("rollback to " + savepointId),
							DBEmptyTextResultConsumer.INSTANCE);
				}
			}
			
			
//			if (hasResults) {
//				ResultChunkManager rcm = new ResultChunkManager(stmt.getResultSet(), Host.getProperties(), "worker", command); 
//				w.setChunkManager( rcm );
//				rsmd = rcm.getMetaData();
//			}
//			else
				rowCount = resultConsumer.getUpdateCount();
			
			resp = new ExecuteResponse(hasResults, rowCount, rsmd ).from(w.getAddress()).success();			
		} catch (PEException pe) {
			anyException = pe;
			throw pe;
		} finally {
			if (logger.isDebugEnabled())
				logger.debug(new StringBuilder("WorkerExecuteRequest/w(").append(w.getName()).append("/").append(w.getCurrentDatabaseName()).append("): exec'd \"")
						.append(stmtCommand).append("\" updating ").append(rowCount).append(" rows (hasResults=")
						.append(hasResults ? "true" : "false")
						.append(")").append(" except=")
						.append(anyException == null ? "none" : anyException.getMessage())						
						.toString());
		}
		return resp;
	}

	@Override
	public String toString() {
		return new StringBuffer().append("WorkerExecuteRequest("+getCommand()+")").toString();
	}

	public SQLCommand getCommand() {
		return command;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.W_EXECUTE_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}


	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return new LogSiteStatisticRequest(OperationClass.EXECUTE);
	}
	
	protected State getImplState(EventStateMachine esm, AbstractEvent event) {
		return new SimpleExecuteState((WorkerExecuteRequestEvent)event);
	}

	protected abstract class WorkerStep extends SequentialStep {
		
		protected final WorkerExecuteRequestEvent rootRequest;
		protected SequentialStep nextStep = null;
		
		public WorkerStep(WorkerExecuteRequestEvent origReq) {
			rootRequest = origReq;
		}
		
		public Worker getWorker() {
			return rootRequest.getWorker();
		}

		public DBResultConsumer getConsumer() {
			return rootRequest.getConsumer();
		}
		
		@Override
		public String getName() {
			return getClass().getName();
		}
		
		protected abstract WorkerExecuteEvent buildExecuteEvent();
		
		@Override
		public SequentialTransition onExecute(EventStateMachine esm) {
			return new SequentialTransition()
				.withSequentialEvent(buildExecuteEvent(), getWorker());
		}
		
		@Override
		public SequentialTransition onResponse(Request originalRequest, EventStateMachine esm,
				Response response, Request forRequest) {
			if (response.isError()) {
				return propagateResponse(esm, originalRequest, response);
			} else {
				SequentialStep myNextStep = getNextStep();
				if (myNextStep != null) {
					return new SequentialTransition()
						.withNextStep(myNextStep);
				} else {
					// no next step, we are done.  send along an ack
					return new SequentialTransition()
						.withSequentialTransition(null, StackAction.POP)
						.withSequentialEvent(new AcknowledgeResponse(this,originalRequest),
								originalRequest.getResponseTarget());
				}
			}
		}
		
		public void setNextStep(SequentialStep ws) {
			nextStep = ws;
		}
		
		public SequentialStep getNextStep() {
			return nextStep;
		}
		
	}
	
	protected class SetDatabaseStep extends WorkerStep {

		public SetDatabaseStep(WorkerExecuteRequestEvent req) {
			super(req);
		}
		
		
		@Override
		protected WorkerExecuteEvent buildExecuteEvent() {
			return new WorkerExecuteEvent(this,rootRequest) {

				@Override
				public void execute(Worker w) throws SQLException, PEException, XAException {
					w.setCurrentDatabase(defaultDatabase);
				}
				
			};
		}

		
	}
	
	protected class StartTransactionStep extends WorkerStep {

		
		public StartTransactionStep(WorkerExecuteRequestEvent req) {
			super(req);
		}

		@Override
		protected WorkerExecuteEvent buildExecuteEvent() {
			return new WorkerExecuteEvent(this,rootRequest) {

				@Override
				public void execute(Worker w) throws SQLException, PEException, XAException {
					w.startTrans(getTransId());
				}
				
			};
		}

		
	}
	
	protected class SavepointStep extends WorkerStep {

		private String savepointId;
		private String prefix;
		
		public SavepointStep(WorkerExecuteRequestEvent req, String prefix, String id) {
			super(req);
			savepointId = id;
			this.prefix = prefix;
		}

		@Override
		protected WorkerExecuteEvent buildExecuteEvent() {
			return new WorkerExecuteEvent(this,rootRequest) {

				@Override
				public void execute(Worker w) throws SQLException, PEException,
						XAException {
					w.getStatement().execute(getConnectionId(), 
							new SQLCommand(String.format("%s %s",prefix,savepointId)),
							DBEmptyTextResultConsumer.INSTANCE);
				}
				
			};
		}
		
	}
	
	protected class ExecuteOriginalStatementStep extends WorkerStep {
		
		private boolean hasResults = false;
		private final SQLCommand stmtCommand;
		private WorkerStatement stmt;
		
		public ExecuteOriginalStatementStep(WorkerExecuteRequestEvent req, SQLCommand stmtCommand) {
			super(req);
			this.stmtCommand = stmtCommand;
		}

		@Override
		protected WorkerExecuteEvent buildExecuteEvent() {
			return new WorkerExecuteEvent(this,rootRequest) {

				@Override
				public void execute(Worker w) throws SQLException, PEException,
						XAException {
					stmt = w.getStatement();
					hasResults = stmt.execute(getConnectionId(), stmtCommand, getConsumer());
				}
				
			};
		}
		
		public boolean getHasResults() {
			return hasResults;
		}
		
		public WorkerStatement getWorkerStatement() {
			return stmt;
		}
	}
	
	private class ReturnResponseStep extends SequentialStep {

		private final WorkerExecuteRequestEvent rootRequest;
		private final ExecuteOriginalStatementStep execStep;

		public ReturnResponseStep(WorkerExecuteRequestEvent root, ExecuteOriginalStatementStep exec) {
			this.rootRequest = root;
			this.execStep = exec;
		}
		
		@Override
		public SequentialTransition onExecute(EventStateMachine esm) {
			try {
				ResponseMessage rm = new ExecuteResponse(execStep.getHasResults(),rootRequest.getConsumer().getUpdateCount(),null).success();
				return new SequentialTransition()
				.withSequentialEvent(new WrappedResponseMessageEvent(this,rootRequest,rm), rootRequest.getResponseTarget())
				.withSequentialTransition(null, StackAction.POP);
			} catch (Throwable t) {
				return propagateRequestException(rootRequest, t);
			}
		}

		@Override
		public SequentialTransition onResponse(Request originalRequest,
				EventStateMachine esm, Response response, Request forRequest) {
			throw new RuntimeException("Illegal response call");
		}

		@Override
		public String getName() {
			return "ReturnResponseStep";
		}
		
	}
	
	private class WrapupStep extends SequentialStep {

		private final WorkerExecuteRequestEvent rootRequest;
		private final ExecuteOriginalStatementStep execStep;
		private final String savepointId;

		public WrapupStep(WorkerExecuteRequestEvent origRequest, ExecuteOriginalStatementStep execStep, String savepoint) {
			this.rootRequest = origRequest;
			this.execStep = execStep;
			this.savepointId = savepoint;
		}
		
		@Override
		public SequentialTransition onExecute(EventStateMachine esm) {
			ReturnResponseStep rrs = new ReturnResponseStep(rootRequest,execStep);
			
			if (recoverLocks) {
				try {
					boolean rowsFound = 
							(execStep.getHasResults() && execStep.getWorkerStatement().getResultSet().isBeforeFirst())
							|| (!execStep.getHasResults() && rootRequest.getConsumer().getUpdateCount() > 0);
					if (!rowsFound) {
						WorkerStep nextStep = new SavepointStep(rootRequest,"rollback to",savepointId);
						nextStep.setNextStep(rrs);
						return new SequentialTransition()
							.withNextStep(nextStep);
					}
				} catch (Throwable t) {
					return propagateRequestException(rootRequest,t);
				}
			}

			return new SequentialTransition()
				.withNextStep(rrs);
		}

		@Override
		public SequentialTransition onResponse(Request originalRequest,
				EventStateMachine esm, Response response, Request forRequest) {
			throw new RuntimeException("Illegal call");
		}

		@Override
		public String getName() {
			return "WrapupStep";
		}

		
	}
	
	private class SimpleExecuteState extends SequentialState {

		private WorkerExecuteRequestEvent execRequest;

		public SimpleExecuteState(WorkerExecuteRequestEvent workerExec) {
			super(SequentialExecutionMode.FIRSTFAILURE);
			execRequest = workerExec;
		}
		
		@Override
		protected SequentialStep buildFirstStep(EventStateMachine esm,
				Request request) {
			Worker w = execRequest.getWorker();
			String savepointId = null;
			WorkerStep first = new SetDatabaseStep(execRequest);
			WorkerStep current = first;
			if (isAutoTransact()) {
				current.setNextStep(new StartTransactionStep(execRequest));
				current = (WorkerStep) current.getNextStep();
			}
			if (recoverLocks) {
				savepointId = "barrier" + w.getUniqueValue();
				current.setNextStep(new SavepointStep(execRequest,"savepoint",savepointId));
				current = (WorkerStep) current.getNextStep();
			}
			ExecuteOriginalStatementStep execStep =
					new ExecuteOriginalStatementStep(execRequest,getCommand());
			current.setNextStep(execStep);
			current = execStep;
			current.setNextStep(new WrapupStep(execRequest,execStep,savepointId));
			
			return first;
		}

		@Override
		public String getName() {
			return "WorkerExecuteRequest.SimpleExecuteState";
		}
		
	}
	
	
}
