// OS_STATUS: public
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

import com.tesora.dve.db.mysql.FieldMetadataAdapter;
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.exceptions.PECodingException;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.mysql.MysqlExecuteCommand;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlTextResultCollector extends MysqlParallelResultConsumer {

	boolean needsMetadata = false;
	private int fieldCount;
	
	protected ColumnSet columnSet = null;
	
	List<ArrayList<String>> rowData = null;
	private boolean moreData = true;
	protected byte sequenceId = 0;
	private long resultsLimit = Long.MAX_VALUE;
	
	public MysqlTextResultCollector() {
		super.setSenderCount(1);
	}

	public MysqlTextResultCollector(boolean needsMetadata) {
		this();
		this.needsMetadata = needsMetadata;
	}

//	public MysqlResultCollector(WorkerGroup wg) {
//		super.setSenderCount(wg.size());
//	}

	@Override
	public void consumeEmptyResultSet(MyOKResponse ok) {
		moreData = false;
	}

	@Override
	public void consumeError(MyErrorResponse errorResp) throws PEException {
        throw errorResp.asException();
	}

	@Override
	public void consumeFieldCount(MyColumnCount colCount) {
		this.fieldCount = colCount.getColumnCount();
		if (needsMetadata) {
			columnSet = new ColumnSet();
		}
	}

    @Override
    public void consumeField(int field_, MyFieldPktResponse columnDef, ColumnInfo columnInfo) {
		if (needsMetadata) {
            ColumnMetadata columnMeta = FieldMetadataAdapter.buildMetadata(columnDef);
			if (columnInfo != null) {
                columnMeta.setName(columnInfo.getName());
                columnMeta.setAliasName(columnInfo.getAlias());
			}
			columnSet.addColumn(columnMeta);
		}
	}

	@Override
	public void consumeFieldEOF(MyMessage unknown) {
		// no nothing ?
	}


    public void consumeRowText(MyTextResultRow textRow) {
        if (getRowDataList().size() >= resultsLimit)
            return;

        ArrayList<String> row = new ArrayList<String>(fieldCount);
        for (int i=0;i< textRow.size();i++){
            row.add(textRow.getString(i));
        }

        getRowDataList().add(row);
    }

    @Override
    public void consumeRowBinary(MyBinaryResultRow binRow) throws PEException {
        throw new PECodingException("Didn't expect binary results in " + this.getClass().getSimpleName());
    }

    @Override
	public void consumeRowEOF() {
		setNumRowsAffected(getRowData().size());
		moreData  = false;
	}

	protected List<ArrayList<String>> getRowDataList() {
		if (rowData == null)
			rowData = new LinkedList<ArrayList<String>>();
		return rowData;
	}

	public int getFieldCount() {
		return fieldCount;
	}

	public List<ArrayList<String>> getRowData() {
		return getRowDataList();
	}

	public boolean isMoreData() {
		return moreData;
	}

	public byte getSequenceId() {
		return sequenceId;
	}

	@Override
	public void setResultsLimit(long resultsLimit) {
		this.resultsLimit = resultsLimit;
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows) throws PEException {
		for (ResultRow row : rows) {
			List<ResultColumn> colData = row.getRow();
			ArrayList<String> newRowData = new ArrayList<String>(colData.size());
			for (ResultColumn col : colData) {
				newRowData.add(col.toString());
			}
			getRowData().add(newRowData);
			if (rows.size() >= resultsLimit)
				break;
		}
		setNumRowsAffected(rows.size());
		setHasResults();
	}

	public void printRows() {
//		for(ArrayList<String> row : rowData) {
//			for (String val : row) {
//				System.out.print(val + '\t');
//			}
//			System.out.println();
//		}
	}

	@Override
	public PEFuture<Boolean> writeCommandExecutor(Channel channel, StorageSite site, DBConnection.Monitor connectionMonitor, SQLCommand sql, PEPromise<Boolean> promise) {
		channel.write(new MysqlExecuteCommand(sql, connectionMonitor, this, promise));
		return promise;
	}

	public ColumnSet getColumnSet() {
		return columnSet;
	}

	public long getResultsLimit() {
		return resultsLimit;
	}

	@Override
	public void setNumRowsAffected(long numRowsAffected) {
		super.setNumRowsAffected(numRowsAffected);
	}
}
