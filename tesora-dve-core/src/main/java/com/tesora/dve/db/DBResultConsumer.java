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

import java.util.List;

import com.tesora.dve.exceptions.PECodingException;
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

    //****************************************
    //Oriented around inspecting results after request dispatch..
    //****************************************
    public boolean hasResults() {
        return false;
    }

    public boolean isSuccessful() {return false;}

    public long getUpdateCount() throws PEException {return 0L;}

    //****************************************
    //Oriented around changing processing behavior before request dispatch.
    //****************************************
    public void setRowAdjuster(RowCountAdjuster rowAdjuster){}
    public void setResultsLimit(long resultsLimit){}

    //****************************************
    //Oriented around changing behavior after request dispatch.
    //****************************************
    public void inject(ColumnSet metadata, List<ResultRow> rows) throws PEException {
        throw new PECodingException(this.getClass().getSimpleName()+".inject not supported");
    }

    public void setNumRowsAffected(long rowcount){}

    public void rollback(){}

    //****************************************
    //Oriented around actual dispatch of requests to databases.
    //****************************************
    @Override
    public void setSenderCount(int senderCount){}

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
