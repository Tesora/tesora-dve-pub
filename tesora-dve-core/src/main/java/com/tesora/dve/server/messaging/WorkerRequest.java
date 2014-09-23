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

import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.DelegatingCompletionHandle;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.worker.WorkerStatement;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.comms.client.messages.RequestMessage;
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

    public final void executeRequest(Worker w, DBResultConsumer resultConsumer) throws SQLException, PEException, XAException {
        try {
            PEDefaultPromise<Boolean> promise = new PEDefaultPromise<>();
            executeRequest(w,resultConsumer,promise);
            promise.sync();
        } catch (Exception e) {
            if (e instanceof PEException)
                throw (PEException)e;
            if (e instanceof SQLException)
                throw (SQLException)e;
            if (e instanceof XAException)
                throw (XAException)e;
            else
                throw new PEException(e);
        }
    }

	public abstract void executeRequest(Worker w, DBResultConsumer resultConsumer, CompletionHandle<Boolean> promise);

    protected void simpleExecute(final Worker w, final DBResultConsumer resultConsumer, SQLCommand ddl, final CompletionHandle<Boolean> promise) {
        if (WorkerDropDatabaseRequest.logger.isDebugEnabled()) {
            WorkerDropDatabaseRequest.logger.debug(w.getName()+": Current database is "+ w.getCurrentDatabaseName());
            WorkerDropDatabaseRequest.logger.debug(w.getName()+":executing statement " + ddl);
        }

        CompletionHandle<Boolean> resultTracker = new DelegatingCompletionHandle<Boolean>(promise) {
            @Override
            public void success(Boolean returnValue) {
                super.success(returnValue);
            }

            @Override
            public void failure(Exception e) {
                if (e instanceof PEException)
                    super.failure(e);
                else {
                    PEException convert = new PEException(e);
                    convert.fillInStackTrace();
                    super.failure(convert);
                }
            }
        };

        WorkerStatement stmt = null;
        try {
            stmt = w.getStatement();
            stmt.execute(getConnectionId(), ddl, resultConsumer,resultTracker);
        } catch (PESQLException e) {
            resultTracker.failure(e);
        }
    }

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
