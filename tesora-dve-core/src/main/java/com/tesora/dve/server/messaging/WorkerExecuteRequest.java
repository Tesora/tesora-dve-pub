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


import java.sql.ResultSet;
import java.sql.SQLException;

import javax.transaction.xa.XAException;

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.PEDefaultPromise;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
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
	public void executeRequest(Worker w, DBResultConsumer resultConsumer, CompletionHandle<Boolean> promise) {
		executeStatement(w, getCommand(), resultConsumer, promise);
	}
	
	protected void executeStatement(final Worker w, final SQLCommand stmtCommand, final DBResultConsumer resultConsumer, final CompletionHandle<Boolean> callersResult) {

        try {
            w.setCurrentDatabase(defaultDatabase);

            // do any late resolution

            if (isAutoTransact())
                w.startTrans(getTransId());
		

			String savepointId = null;
			
			if (recoverLocks) {
                savepointId = executeSavepoint(w);
			}

            final String finalSavepoint = savepointId;

			final WorkerStatement stmt = w.getStatement();

            CompletionHandle<Boolean> executeTracker = new PEDefaultPromise<Boolean>(){
                @Override
                public void failure(Exception t) {
                    callersResult.failure(t);
                }

                @Override
                public void success(Boolean returnValue) {
                    try {
                        long rowCount = -1;
                        final boolean hasResults = false;
                        ColumnSet rsmd = null;

                        if (recoverLocks) {
                            ResultSet resultSet = stmt.getResultSet();
                            boolean rowsFound = (hasResults && resultSet != null && resultSet.isBeforeFirst())
                                    || (!hasResults && resultConsumer.getUpdateCount() > 0);
                            if (!rowsFound) {
                                rollbackToSavepoint(w, finalSavepoint);
                            }
                        }

                        rowCount = resultConsumer.getUpdateCount();
                        new ExecuteResponse(hasResults, rowCount, rsmd ).from(w.getAddress()).success();
                        callersResult.success(true);
                    } catch (Exception e){
                        callersResult.failure(e);
                    }
                }

            };

            stmt.execute(getConnectionId(), stmtCommand, resultConsumer,executeTracker);
		} catch (Exception pe) {
			callersResult.failure(pe);
		} finally {
            //SMG: need to replace this logging behavior.
//			if (logger.isDebugEnabled())
//				logger.debug(new StringBuilder("WorkerExecuteRequest/w(").append(w.getName()).append("/").append(w.getCurrentDatabaseName()).append("): exec'd \"")
//						.append(stmtCommand).append("\" updating ").append(rowCount).append(" rows (hasResults=")
//						.append(hasResults ? "true" : "false")
//						.append(")").append(" except=")
//						.append(anyException == null ? "none" : anyException.getMessage())
//						.toString());
		}
	}

    private void rollbackToSavepoint(Worker w, String savepointId) throws PEException {
        try {
            PEDefaultPromise<Boolean> promise = new PEDefaultPromise<>();
            w.getStatement().execute(getConnectionId(), new SQLCommand("rollback to " + savepointId), DBEmptyTextResultConsumer.INSTANCE,promise);
            promise.sync();
        } catch (Exception e) {
            throw new PEException(e);
        }
    }

    private String executeSavepoint(Worker w) throws PEException {
        String savepointId;
        savepointId = "barrier" + w.getUniqueValue();

        try {
            PEDefaultPromise<Boolean> promise = new PEDefaultPromise<>();
            w.getStatement().execute(getConnectionId(), new SQLCommand("savepoint " + savepointId), DBEmptyTextResultConsumer.INSTANCE, promise);
            promise.sync();
        } catch (Exception e) {
            throw new PEException(e);
        }
        return savepointId;
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
}
