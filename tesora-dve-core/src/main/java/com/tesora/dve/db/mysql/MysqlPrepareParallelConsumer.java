package com.tesora.dve.db.mysql;

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
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.MSPComPrepareStmtRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public abstract class MysqlPrepareParallelConsumer extends DBResultConsumer {

	boolean successful = false;
	
	final MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt =
			new MyPreparedStatement<MysqlGroupedPreparedStatementId>(new MysqlGroupedPreparedStatementId());
	
	private int numParams = 0;
	private short warnings = 0;
	private int numCols = 0;
	private ChannelHandlerContext ctxToConsume = null;

    @Override
    public void writeCommandExecutor(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
        MysqlMessage message = MSPComPrepareStmtRequestMessage.newMessage(sql.getSQL(), channel.getTargetCharset());
		MysqlCommand cmd = new MysqlStmtPrepareCommand(channel, sql.getSQL(), message, this, promise);
        channel.writeAndFlush( cmd );
	}

	public void header(CommandChannel executingOnChannel, ChannelHandlerContext ctx, MyPrepareOKResponse prepareOK) {
		pstmt.getStmtId().addStmtId(executingOnChannel.getPhysicalID(), (int)prepareOK.getStmtId());
		synchronized (this) {
			if (ctxToConsume == null) {
				ctxToConsume  = ctx;
				//			stmtId = wholePacket.readInt();
				numCols = prepareOK.getNumColumns();
				numParams = prepareOK.getNumParams();
				warnings = (short) prepareOK.getWarningCount();
				successful = true;
				consumeHeader(prepareOK);
			}
		}
	}

	@Override
	public void rollback() {
		numParams = 0;
		numCols = 0;
		warnings = 0;
	}

    abstract void consumeHeader(MyPrepareOKResponse prepareOK);

	public void paramDef(ChannelHandlerContext ctx, MyFieldPktResponse paramDef) throws PEException {
		if (ctx == ctxToConsume){
			consumeParamDef(paramDef);
        }
	}

    abstract void consumeParamDef(MyFieldPktResponse paramDef) throws PEException;

	public void paramDefEOF(ChannelHandlerContext ctx, MyEOFPktResponse paramEof) {
		if (ctx == ctxToConsume) {
			consumeParamDefEOF(paramEof);
		}
	}

    abstract void consumeParamDefEOF(MyEOFPktResponse paramEof);

	public void colDef(ChannelHandlerContext ctx, MyFieldPktResponse columnDef) {
		if (ctx == ctxToConsume) {
			consumeColDef(columnDef);
		}
	}

    abstract void consumeColDef(MyFieldPktResponse colDef);

	public void colDefEOF(ChannelHandlerContext ctx, MyEOFPktResponse colEof) {
		if (ctx == ctxToConsume) {
			consumeColDefEOF(colEof);
		}
	}

    abstract void consumeColDefEOF(MyEOFPktResponse colEof);

	public void errorResponse(ChannelHandlerContext ctx, MyErrorResponse error) throws PESQLStateException {
		consumeError(error);
	}

    abstract void consumeError(MyErrorResponse error) throws PESQLStateException;

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
	public void inject(ColumnSet metadata, List<ResultRow> rows)
			throws PEException {
		throw new PECodingException(this.getClass().getSimpleName()+".inject not implemented");
	}

	@Override
	public void setRowAdjuster(RowCountAdjuster rowAdjuster) {
	}

	@Override
	public void setNumRowsAffected(long rowcount) {
	}

	@Override
	public boolean isSuccessful() {
		return successful;
	}

	public short getWarnings() {
		return warnings;
	}

	public int getNumParams() {
		return numParams;
	}

	public int getNumCols() {
		return numCols;
	}

	public MyPreparedStatement<MysqlGroupedPreparedStatementId> getPreparedStatement() {
		return pstmt;
	}

}
