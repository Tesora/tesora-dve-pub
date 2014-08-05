package com.tesora.dve.db;

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

import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.mysql.MysqlForwardedExecuteCommand;
import com.tesora.dve.db.mysql.RedistTupleBuilder;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;

public class RedistTupleUpdateConsumer implements DBResultConsumer {
	
	static final Logger logger = Logger.getLogger(RedistTupleUpdateConsumer.class);

	RedistTupleBuilder forwardedResultHandler;
    SynchronousCompletion<RedistTupleBuilder> readySynchronizer;

	final Future<SQLCommand> insertStatementFuture;
	final SQLCommand insertOptions;
	final WorkerGroup targetWG;
	final PersistentTable targetTable;
	final int maxTupleCount;
	final int maxDataSize;
	boolean insertIgnore = false;

	
	public RedistTupleUpdateConsumer(
			Future<SQLCommand> insertStatementFuture, SQLCommand insertOptions, 
			PersistentTable targetTable, int maxTupleCount, int maxDataSize, WorkerGroup targetWG) {
		this.insertOptions = insertOptions;
		this.insertStatementFuture = insertStatementFuture;
		this.targetTable = targetTable;
		this.maxTupleCount = maxTupleCount;
		this.targetWG = targetWG;
		this.maxDataSize = maxDataSize;
	}

    @Override
    public void writeCommandExecutor(Channel channel, StorageSite site, DBConnection.Monitor connectionMonitor, SQLCommand sql, CompletionHandle<Boolean> promise) {
		MysqlForwardedExecuteCommand execCommand =
				new MysqlForwardedExecuteCommand(forwardedResultHandler, promise, site);
		channel.write(execCommand);
		if (logger.isDebugEnabled())
			logger.debug(channel + " <== " + execCommand);
		promise.success(false);
	}
	
	public RedistTupleBuilder getExecutionHandler() throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("About to call readyPromise.sync(): " + readySynchronizer);
		return readySynchronizer.sync();
	}
	
	public SynchronousCompletion<RedistTupleBuilder> getHandlerFuture() {
		return readySynchronizer;
	}

	@Override
	public void setSenderCount(int senderCount) {
        PECountdownPromise<RedistTupleBuilder> countdownResult = new PECountdownPromise<RedistTupleBuilder>(senderCount);

        readySynchronizer = countdownResult;

        forwardedResultHandler = new RedistTupleBuilder(insertStatementFuture, insertOptions, targetTable, maxTupleCount, maxDataSize, countdownResult, targetWG);
		forwardedResultHandler.setInsertIgnore(insertIgnore);
	}

	@Override
	public boolean hasResults() {
		return false;
	}

	public long getRowsUpdatedCount() throws PEException {
		try {
			return getExecutionHandler().getUpdateCount();
		} catch (Exception e) {
			throw new PEException(e);
		}
	}

	@Override
	public void setResultsLimit(long resultsLimit) {
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows)
			throws PEException {
	}

	@Override
	public void setRowAdjuster(RowCountAdjuster rowAdjuster) {
	}

	@Override
	public void setNumRowsAffected(long rowcount) {
	}

	@Override
	public boolean isSuccessful() {
		return false;
	}

	@Override
	public long getUpdateCount() throws PEException {
		return 0;
	}

	@Override
	public void rollback() {
	}

	public void setInsertIgnore(boolean insertIgnore) {
		this.insertIgnore = insertIgnore;
	}

}
