// OS_STATUS: public
package com.tesora.dve.db;


import io.netty.channel.Channel;

import java.util.List;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
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

	PEFuture<Boolean> writeCommandExecutor(Channel channel, StorageSite site, DBConnection.Monitor connectionMonitor, SQLCommand sql, PEPromise<Boolean> promise);

	boolean isSuccessful();

	void rollback();
}
