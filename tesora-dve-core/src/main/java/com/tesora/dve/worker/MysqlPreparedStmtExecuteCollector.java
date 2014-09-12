package com.tesora.dve.worker;

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
import com.tesora.dve.db.mysql.FieldMetadataAdapter;
import com.tesora.dve.db.mysql.MysqlCommand;
import com.tesora.dve.db.mysql.libmy.*;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.MysqlQueryResultConsumer;
import com.tesora.dve.db.ResultChunkProvider;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.common.DataTypeValueFunc;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import com.tesora.dve.db.mysql.MysqlStmtExecuteCommand;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlPreparedStmtExecuteCollector extends DBResultConsumer implements MysqlQueryResultConsumer, ResultChunkProvider {
	
	static Logger logger = Logger.getLogger(MysqlPreparedStmtExecuteCollector.class);

	private int fieldCount;

	private MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt;

	boolean hasResults = false;
	long numRowsAffected = 0;
	ResultChunk chunk = new ResultChunk();
	private ColumnSet resultColumnMetadata = new ColumnSet();
	int fieldIndex;

	List<DataTypeValueFunc> columnInspectorList;
	
	public MysqlPreparedStmtExecuteCollector(MyPreparedStatement<MysqlGroupedPreparedStatementId> myPreparedStatement) 
	{
		this.pstmt = myPreparedStatement;
		
		if ( logger.isDebugEnabled() )
			logger.debug("Prepared Stmt Execute for: " + pstmt );
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows)
			throws PEException {
		throw new PECodingException(this.getClass().getSimpleName()+".inject not supported");
	}

    @Override
    public MysqlCommand writeCommandExecutor(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
		return new MysqlStmtExecuteCommand(sql, channel.getMonitor(), pstmt, sql.getParameters(), this, promise);
	}

	@Override
	public boolean hasResults() {
		return hasResults;
	}
	
	@Override
	public long getUpdateCount() throws PEException {
		return 0;
	}
	
	@Override
	public void setResultsLimit(long resultsLimit) {
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

    public boolean emptyResultSet(MyOKResponse ok) {
        numRowsAffected = ok.getAffectedRows();
        return numRowsAffected > 0;
    }

    @Override
    public void active(ChannelHandlerContext ctx) {
        //NOOP.
    }

    @Override
	public void error(MyErrorResponse err) throws PEException {
			throw err.asException();
	}

    @Override
    public void fieldCount(MyColumnCount colCount) {
		hasResults = true; 
		this.fieldCount = colCount.getColumnCount();
	}

	@Override
	public void fieldEOF(MyMessage unknown)	throws PEException {
		if (columnInspectorList == null) {
			columnInspectorList = DBTypeBasedUtils.getMysqlTypeFunctions(resultColumnMetadata);
			chunk.setColumnSet(resultColumnMetadata);
		}
	}
	
	@Override
	public void rowEOF(MyEOFPktResponse wholePacket) throws PEException {
	}

    @Override
    public void rowText(MyTextResultRow textRow) throws PEException {
        throw new PECodingException("Didn't expect text results in " + this.getClass().getSimpleName());
    }

    @Override
    public void rowBinary(MyBinaryResultRow binRow) throws PEException {
        ResultRow resultRow = new ResultRow();
        for (int i = 0; i < columnInspectorList.size(); ++i) {
            resultRow.addResultColumn(new ResultColumn( binRow.getValue(i) ));
        }
        chunk.addResultRow(resultRow);
    }

    @Override
    public void rowFlush() throws PEException {
        //ignored.
    }


    @Override
	public void field(int fieldIndex, MyFieldPktResponse columnDef, ColumnInfo columnProjection)
			throws PEException {
		if (this.fieldIndex == fieldIndex) {
            ColumnMetadata columnMeta = FieldMetadataAdapter.buildMetadata(columnDef);
			columnMeta.setOrderInTable(this.fieldIndex);
			resultColumnMetadata.addColumn(columnMeta);
			++this.fieldIndex;
		}
	}
	
	public ColumnSet getColumnSet() {
		return chunk.getColumnSet();
	}

	public Object getSingleColumnValue(int rowIndex, int columnIndex) throws PEException {
		return chunk.getSingleValue(rowIndex, columnIndex).getColumnValue();
	}
	
	public ResultChunk getResultChunk() {
		return chunk;
	}

	@Override
	public void rollback() {
	}

	@Override
	public void setSenderCount(int senderCount) {
	}

	@Override
	public long getLastInsertId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNumRowsAffected() {
		// TODO Auto-generated method stub
		return 0;
	}
}
