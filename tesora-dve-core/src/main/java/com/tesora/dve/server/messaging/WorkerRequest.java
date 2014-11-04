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

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.GroupDispatch;
import com.tesora.dve.db.mysql.DelegatingResultsProcessor;
import com.tesora.dve.db.mysql.MysqlMessage;
import com.tesora.dve.db.mysql.PassFailProcessor;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.worker.StatementManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.comms.client.messages.RequestMessage;
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
public abstract class WorkerRequest extends RequestMessage implements GroupDispatch {

    GroupDispatch groupDispatch;
	final SSContext connectionContext;
	
	boolean autoTransact = true;

    public WorkerRequest withGroupDispatch(GroupDispatch groupDispatch) {
        this.groupDispatch = groupDispatch;
        return this;
    }

    @Override
    public void setSenderCount(int senderCount) {
        this.groupDispatch.setSenderCount(senderCount);
    }

    @Override
    public Bundle getDispatchBundle(CommandChannel connection, SQLCommand sql, CompletionHandle<Boolean> promise) {
        return this.groupDispatch.getDispatchBundle(connection, sql, promise);
    }

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

	public abstract void executeRequest(Worker w, CompletionHandle<Boolean> promise);


    public void execute(final Worker targetWorker, final SQLCommand sql, CompletionHandle<Boolean> promise) {
        //NOTE: all worker requests that need to execute statements should come through here to make things easy to refactor.
        final int connectionId = getConnectionId();
        final GroupDispatch resultConsumer = this;
        final Worker.SingleDirectStatement statement;
        try {
            statement = targetWorker.getStatement();
            CommandChannel dbConnection = statement.dbConnection;

            Long referenceTime = null;
            if (sql.hasReferenceTime())
                referenceTime = sql.getReferenceTime();

            SQLCommand resolvedSQL = sql.getResolvedCommand(targetWorker);

            Bundle dispatchBundle = resultConsumer.getDispatchBundle(dbConnection, resolvedSQL, promise);
            MysqlMessage outboundMessage = dispatchBundle.outboundMessage;


            final DelegatingResultsProcessor statementInterceptor = new DelegatingResultsProcessor(dispatchBundle.resultsProcessor){
                boolean ended = false;
                @Override
                public void active(ChannelHandlerContext ctx) {
                    statementStart(connectionId, sql, statement);
                    super.active(ctx);
                }

                @Override
                public void end(ChannelHandlerContext ctx) {
                    if (!ended) {
                        statementEnd(connectionId, statement);
                        super.end(ctx);
                        ended = true;
                    }
                }

                @Override
                public void failure(Exception e) {
                    if (!ended) {
                        statementEnd(connectionId, statement);
                        PESQLException psqlError = new PESQLException(e.getMessage(), new PESQLException("On statement: " + sql.getDisplayForLog(), e));
                        super.failure(psqlError);
                        ended = true;
                    }
                }
            };

            //TODO: this is one of the null promises that gets deferred. -sgossard
            if (referenceTime != null) {
                CompletionHandle<Boolean> deferred = dbConnection.getExceptionDeferringPromise();
                String setTimestampSQL = "SET TIMESTAMP=" + referenceTime + ";";
                MysqlMessage message = MSPComQueryRequestMessage.newMessage(new SQLCommand(CharsetUtil.UTF_8, setTimestampSQL).getBytes());
                dbConnection.writeAndFlush(message, new PassFailProcessor(deferred));
            }

            targetWorker.writeAndFlush(dbConnection, outboundMessage, statementInterceptor);
        } catch (PEException pe){
            promise.failure(pe);
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

    public static void statementEnd(int connectionId, Worker.SingleDirectStatement executingStatement) {
        StatementManager.INSTANCE.unregisterStatement(connectionId, executingStatement);
        PerHostConnectionManager.INSTANCE.resetConnectionState(connectionId);
    }

    public static void statementStart(int connectionId, SQLCommand sql, Worker.SingleDirectStatement executingStatement) {
        StatementManager.INSTANCE.registerStatement(connectionId, executingStatement);
        PerHostConnectionManager.INSTANCE.changeConnectionState(
                connectionId, "Query", "", (sql == null) ? "Null Query" : sql.getRawSQL());
    }
}
