// OS_STATUS: public
package com.tesora.dve.db.mysql;

import io.netty.channel.Channel;

import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.DBConnection.Monitor;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlKillResultDiscarder implements DBResultConsumer {

	static Logger logger = Logger.getLogger(MysqlKillResultDiscarder.class);

	public static final MysqlKillResultDiscarder INSTANCE = new MysqlKillResultDiscarder();

	@Override
	public void setSenderCount(int senderCount) {
		// no op
	}

	@Override
	public boolean hasResults() {
		return false;
	}

	@Override
	public long getUpdateCount() throws PEException {
		return 0;
	}

	@Override
	public void setResultsLimit(long resultsLimit) {
		// no op
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows) throws PEException {
		// no op
	}

	@Override
	public void setRowAdjuster(RowCountAdjuster rowAdjuster) {
		// no op
	}

	@Override
	public void setNumRowsAffected(long rowcount) {
		// no op
	}

	@Override
	public PEFuture<Boolean> writeCommandExecutor(Channel channel, StorageSite site, Monitor connectionMonitor,
			SQLCommand sql, PEPromise<Boolean> promise) {
		if (logger.isDebugEnabled())
			logger.debug(promise + ", " + channel + " write " + sql.getRawSQL());
		channel.write(new MysqlExecuteCommand(sql, connectionMonitor, null, promise));
		return promise;
	}

	@Override
	public boolean isSuccessful() {
		return false;
	}

	@Override
	public void rollback() {
	}

}
