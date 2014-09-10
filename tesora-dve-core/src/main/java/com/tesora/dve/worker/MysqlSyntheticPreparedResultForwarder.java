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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

import com.tesora.dve.db.MysqlQueryResultConsumer;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import com.tesora.dve.db.mysql.MysqlPrepareParallelConsumer;
import com.tesora.dve.db.mysql.MysqlPrepareStatementDiscarder;
import com.tesora.dve.db.mysql.MysqlStmtCloseCommand;
import com.tesora.dve.db.mysql.MysqlStmtExecuteCommand;
import com.tesora.dve.db.mysql.MysqlStmtPrepareCommand;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlSyntheticPreparedResultForwarder extends MysqlDemultiplexingResultForwarder {

	public MysqlSyntheticPreparedResultForwarder(ChannelHandlerContext outboundCtx) {
		super(outboundCtx);
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows)
			throws PEException {
		throw new PEException("Cannot inject into " + this.getClass().getSimpleName());
	}

    @Override
    public void writeCommandExecutor(final Channel channel, final SQLCommand sql, final CompletionHandle<Boolean> promise) {
		final MysqlQueryResultConsumer resultForwarder = this;
		final MysqlPrepareParallelConsumer prepareCollector = new MysqlPrepareStatementDiscarder();
		final PEDefaultPromise<Boolean> preparePromise = new PEDefaultPromise<Boolean>();
		preparePromise.addListener(new CompletionTarget<Boolean>() {
			@Override
			public void success(Boolean returnValue) {
				MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = prepareCollector.getPreparedStatement();
				channel.write(new MysqlStmtExecuteCommand(sql, pstmt, sql.getParameters(), resultForwarder, promise));
//			System.out.println("selectCollector " + pstmt);
				channel.writeAndFlush(new MysqlStmtCloseCommand(pstmt));
			}
			@Override
			public void failure(Exception e) {
				promise.failure(e);
			}
		});
		channel.write(new MysqlStmtPrepareCommand(sql.getSQL(), prepareCollector, preparePromise));
	}

}
