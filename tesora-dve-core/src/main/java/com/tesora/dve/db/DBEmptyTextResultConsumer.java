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
import com.tesora.dve.db.mysql.MysqlMessage;
import com.tesora.dve.db.mysql.libmy.*;

import java.util.List;

import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.MysqlExecuteCommand;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class DBEmptyTextResultConsumer extends DBResultConsumer implements MysqlQueryResultConsumer {
	
	static Logger logger = Logger.getLogger( DBEmptyTextResultConsumer.class );

	public final static DBEmptyTextResultConsumer INSTANCE = new DBEmptyTextResultConsumer();

    @Override
    public void active(ChannelHandlerContext ctx) {
        //NOOP.
    }

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
    public void writeCommandExecutor(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
		if (logger.isDebugEnabled()) logger.debug(promise + ", " + channel + " write " + sql.getRawSQL());
        MysqlMessage message = MSPComQueryRequestMessage.newMessage(sql.getSQLAsBytes());
        channel.writeAndFlush(message, new MysqlExecuteCommand(sql, channel.getMonitor(), this, promise));
	}

}
