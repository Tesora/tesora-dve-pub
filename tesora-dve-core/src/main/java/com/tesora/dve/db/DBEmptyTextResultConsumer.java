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

import com.tesora.dve.db.mysql.libmy.*;

import io.netty.channel.Channel;

import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.db.mysql.MysqlExecuteCommand;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class DBEmptyTextResultConsumer implements MysqlQueryResultConsumer, DBResultConsumer {
	
	static Logger logger = Logger.getLogger( DBEmptyTextResultConsumer.class );

	public final static DBEmptyTextResultConsumer INSTANCE = new DBEmptyTextResultConsumer();

    public boolean emptyResultSet(MyOKResponse ok) {
        return ok.getAffectedRows() > 0;
    }

    @Override
    public void error(MyErrorResponse errorResponse) throws PEException {
        throw new PEException(errorResponse.toString(), errorResponse.asException());
    }

    @Override
    public void fieldCount(MyColumnCount colCount) {
        throw new PECodingException("Results received in " + DBEmptyTextResultConsumer.class.getSimpleName());
    }

	@Override
	public void field(int fieldIndex, MyFieldPktResponse columnDef, ColumnInfo colProjection) {
		throw new PECodingException("Results received in " + DBEmptyTextResultConsumer.class.getSimpleName());
	}

	@Override
	public void fieldEOF(MyMessage unknown) {
		throw new PECodingException("Results received in " + DBEmptyTextResultConsumer.class.getSimpleName());
	}

	@Override
	public void rowEOF(MyEOFPktResponse wholePacket) {
		throw new PECodingException("Results received in " + DBEmptyTextResultConsumer.class.getSimpleName());
	}

    @Override
    public void rowText(MyTextResultRow textRow) throws PEException {
        throw new PECodingException("Didn't expect text results in " + this.getClass().getSimpleName());
    }

    @Override
    public void rowBinary(MyBinaryResultRow binRow) throws PEException {
        throw new PECodingException("Didn't expect binary results in " + this.getClass().getSimpleName());
    }

    @Override
    public void rowFlush() throws PEException {
        //ignored.
    }

	@Override
	public void setSenderCount(int senderCount) {
	}

	@Override
	public boolean hasResults() {
		return false;
	}

	@Override
	public long getUpdateCount() {
		return 0;
	}

	@Override
	public void setResultsLimit(long resultsLimit) {
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows) {
		throw new PECodingException("Results received in " + DBEmptyTextResultConsumer.class.getSimpleName());
	}

	@Override
	public void setRowAdjuster(RowCountAdjuster rowAdjuster) {
	}

	@Override
	public void setNumRowsAffected(long rowcount) {
	}

	@Override
	public PEFuture<Boolean> writeCommandExecutor(Channel channel, StorageSite site, DBConnection.Monitor connectionMonitor, SQLCommand sql, PEPromise<Boolean> promise) {
		if (logger.isDebugEnabled()) logger.debug(promise + ", " + channel + " write " + sql.getRawSQL());
		channel.write(new MysqlExecuteCommand(sql, connectionMonitor, this, promise));
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
