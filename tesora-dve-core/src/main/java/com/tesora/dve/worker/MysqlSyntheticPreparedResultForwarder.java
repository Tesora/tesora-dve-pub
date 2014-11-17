package com.tesora.dve.worker;

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

import com.tesora.dve.concurrent.*;
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.mysql.*;
import com.tesora.dve.db.mysql.portal.protocol.MSPComPrepareStmtRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtExecuteRequestMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.db.MysqlQueryResultConsumer;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlSyntheticPreparedResultForwarder extends MysqlDemultiplexingResultForwarder {

	public MysqlSyntheticPreparedResultForwarder(ChannelHandlerContext outboundCtx) {
		super(outboundCtx);
	}

    @Override
    public Bundle getDispatchBundle(final CommandChannel channel, final SQLCommand sql, final CompletionHandle<Boolean> promise) {
        //TODO: this executor is weird.  It sends a prepare, collects the statement ID, sends an execute, then closes the prepared statement. -sgossard
		final MysqlQueryResultConsumer resultForwarder = this;
		final MysqlPrepareParallelConsumer prepareCollector = new MysqlPrepareStatementDiscarder();
		final PEDefaultPromise<Boolean> preparePromise = new PEDefaultPromise<Boolean>();
		preparePromise.addListener(new CompletionTarget<Boolean>() {
			@Override
			public void success(Boolean returnValue) {
				MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = prepareCollector.getPreparedStatement();
                int preparedID = (int)pstmt.getStmtId().getStmtId(channel.getPhysicalID());
                MysqlMessage message = MSPComStmtExecuteRequestMessage.newMessage(preparedID, pstmt, sql.getParameters());
				channel.write( message, new MysqlStmtExecuteCommand(sql, channel.getMonitor(), pstmt, preparedID, sql.getParameters(), resultForwarder, promise));
//			System.out.println("selectCollector " + pstmt);
                MysqlMessage closeMessage = MSPComStmtCloseRequestMessage.newMessage(preparedID);
				channel.writeAndFlush(closeMessage,new MysqlStmtCloseCommand(preparedID, new PEDefaultPromise<Boolean>()));
			}
			@Override
			public void failure(Exception e) {
				promise.failure(e);
			}
		});

        MysqlMessage prepareMessage = MSPComPrepareStmtRequestMessage.newMessage(sql.getSQL(), channel.lookupCurrentConnectionCharset());
        return new Bundle(prepareMessage, new MysqlStmtPrepareCommand(channel,sql.getSQL(), prepareCollector, preparePromise));
	}

}
