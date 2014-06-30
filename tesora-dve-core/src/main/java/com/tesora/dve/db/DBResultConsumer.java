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


import com.tesora.dve.concurrent.CompletionHandle;
import io.netty.channel.Channel;

import java.util.List;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public interface DBResultConsumer {

	public interface RowCountAdjuster {
		long adjust(long numRowsAffected, int siteCount);
	}

	void setSenderCount(int senderCount);

	boolean hasResults();

	long getUpdateCount() throws PEException;

	void setResultsLimit(long resultsLimit);

	void inject(ColumnSet metadata, List<ResultRow> rows) throws PEException;

	void setRowAdjuster(RowCountAdjuster rowAdjuster);

	void setNumRowsAffected(long rowcount);

    void writeCommandExecutor(Channel channel, StorageSite site, DBConnection.Monitor connectionMonitor, SQLCommand sql, CompletionHandle<Boolean> promise);

	boolean isSuccessful();

	void rollback();
}
