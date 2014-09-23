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
import com.tesora.dve.db.mysql.MysqlCommand;
import com.tesora.dve.db.mysql.MysqlCommandBundle;
import com.tesora.dve.exceptions.PECommunicationsException;
import io.netty.channel.Channel;

import java.util.List;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DBResultConsumer implements GroupDispatch {
    static final Logger logger = LoggerFactory.getLogger(DBResultConsumer.class);

	public interface RowCountAdjuster {
		long adjust(long numRowsAffected, int siteCount);
	}

	abstract public void setSenderCount(int senderCount);

    abstract public boolean hasResults();

    abstract public long getUpdateCount() throws PEException;

    abstract public void setResultsLimit(long resultsLimit);

    abstract public void inject(ColumnSet metadata, List<ResultRow> rows) throws PEException;

    abstract public void setRowAdjuster(RowCountAdjuster rowAdjuster);

    abstract public void setNumRowsAffected(long rowcount);

    abstract public boolean isSuccessful();

    abstract public void rollback();

    abstract public void writeCommandExecutor(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise);

    @Override
    public final void dispatch(CommandChannel connection, SQLCommand sql, CompletionHandle<Boolean> promise) {
        /**TODO: In order to decouple the DBResultConsumer hierarchy from the DBConnection classes, this logic was moved
         * out of MysqlConnection, and unfortunately still has some connection state related dependencies.
         * after all the DBResultConsumer nastiness is untangled, it would be good to move the exception deferring
         * stuff 100% out of the connection, and the isOpen/communication failure stuff 100% back in. -sgossard
          */
        if (promise == null)
            promise = connection.getExceptionDeferringPromise();

        this.writeCommandExecutor(connection, sql, promise);
    }
}
