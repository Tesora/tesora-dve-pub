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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.concurrent.*;

import java.util.List;
import java.util.concurrent.Future;

import com.tesora.dve.db.mysql.MysqlCommand;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.db.mysql.MysqlForwardedExecuteCommand;
import com.tesora.dve.db.mysql.RedistTupleBuilder;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;

public class RedistTupleUpdateConsumer extends DBResultConsumer  {
	
	static final Logger logger = Logger.getLogger(RedistTupleUpdateConsumer.class);

	RedistTupleBuilder forwardedResultHandler;


	public RedistTupleUpdateConsumer(RedistTupleBuilder builder) {
        this.forwardedResultHandler = builder;
    }

    @Override
    public MysqlCommand writeCommandExecutor(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
		MysqlForwardedExecuteCommand execCommand =
				new MysqlForwardedExecuteCommand(channel.getStorageSite(), forwardedResultHandler, promise);
		return execCommand;
	}

	@Override
	public void setSenderCount(int senderCount) {
	}

	@Override
	public boolean hasResults() {
		return false;
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

}
