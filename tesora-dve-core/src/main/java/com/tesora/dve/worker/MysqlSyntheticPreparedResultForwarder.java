// OS_STATUS: public
package com.tesora.dve.worker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.concurrent.PEFuture.Listener;
import com.tesora.dve.db.DBConnection;
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
	public PEFuture<Boolean> writeCommandExecutor(final Channel channel, StorageSite site, final DBConnection.Monitor connectionMonitor, final SQLCommand sql, final PEPromise<Boolean> promise) {
		final MysqlQueryResultConsumer resultForwarder = this;
		final MysqlPrepareParallelConsumer prepareCollector = new MysqlPrepareStatementDiscarder();
		final PEDefaultPromise<Boolean> preparePromise = new PEDefaultPromise<Boolean>();
		preparePromise.addListener(new Listener<Boolean>() {
			@Override
			public void onSuccess(Boolean returnValue) {
				MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = prepareCollector.getPreparedStatement();
				channel.write(new MysqlStmtExecuteCommand(sql, connectionMonitor, pstmt, sql.getParameters(), resultForwarder, promise));
//			System.out.println("selectCollector " + pstmt);
				channel.write(new MysqlStmtCloseCommand(pstmt));
				channel.flush();
			}
			@Override
			public void onFailure(Exception e) {
				promise.failure(e);
			}
		});
		channel.write(new MysqlStmtPrepareCommand(sql.getSQL(), prepareCollector, preparePromise));
		return promise;
	}

}
