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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.SynchronousCompletion;
import io.netty.channel.Channel;

import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.*;
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.MysqlQueryResultConsumer;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.common.DataTypeValueFunc;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.TableHints;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class MysqlRedistTupleForwarder implements MysqlQueryResultConsumer, DBResultConsumer {
	
	static Logger logger = Logger.getLogger(MysqlRedistTupleForwarder.class);

    //these are read outside netty thread, make them volatile.
	volatile int senderCount = 0;
	volatile int rowCount = 0;

	private final SynchronousCompletion<RedistTupleBuilder> handlerFuture;
	private final CatalogDAO catalogDAO;
	private final DistributionModel distModel;
	private final WorkerGroup targetWG;
	private final KeyValue distValue;
	private final TableHints tableHints;
	private final boolean useResultSetAliases;
	
	AtomicReference<RedistTupleBuilder> targetHandler = new AtomicReference<RedistTupleBuilder>();

	private int fieldCount = -1;

	private MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt;

	private ColumnSet resultColumnMetadata = new ColumnSet();
	private int fieldIndex = 0;

	boolean columnInspectorComputed = false;

	// this class gets setup for columns in the result set that we need to inspect the value
	// from the incoming result set. Currently this is columns in a distrubtion vector and specfied
	// auto increment columns
	class ColumnValueInspector {
		DataTypeValueFunc typeReader;
		String dvCol = null;
		boolean isIncrCol = false;
		
		void inspectValue(MyBinaryResultRow binRow, int columnNumber, KeyValue dv, MaximumAutoIncr maxAutoIncr) throws PEException {
            Object value = binRow.getValue(columnNumber);
            boolean columnIsNull = binRow.isNull(columnNumber);
			if (dvCol != null)
				dv.get(dvCol).setValue(value);
			if (isIncrCol)
				if ( columnIsNull )
					throw new PEException("Found NULL value for auto-increment column in table " + dv.getUserTable().getPersistentName());
				else
					maxAutoIncr.setMaxValue(((Number) value).longValue());
		}
	}
	List<ColumnValueInspector> columnInspectorList;
    List<DataTypeValueFunc> typeEncoders;
	
	class MaximumAutoIncr {
		long maxAutoInc = -1;
		
		void setMaxValue( long candidate ) {
			if ( candidate > maxAutoInc )
				maxAutoInc = candidate;
		}
		
		long getMaxValue() {
			return maxAutoInc;
		}

		public boolean isSet() {
			return (maxAutoInc != -1);
		}
	}

	public MysqlRedistTupleForwarder(
			SSContext ssContext, CatalogDAO c, WorkerGroup targetWG, 
			DistributionModel distModel, KeyValue distValue, TableHints tableHints, 
			boolean useResultSetAliases, MyPreparedStatement<MysqlGroupedPreparedStatementId> selectPStatement, 
			SynchronousCompletion<RedistTupleBuilder> handlerFuture)
	{
		this.catalogDAO = c;
		this.distModel = distModel;
		this.targetWG = targetWG;
		this.distValue = distValue;
		this.tableHints = tableHints;
		this.useResultSetAliases = useResultSetAliases;
		this.pstmt = selectPStatement;
		this.handlerFuture = handlerFuture;
		
		if ( logger.isDebugEnabled() )
			logger.debug("Redist: Forwarder setup for: " + distModel + "/" + distValue + ";uRSA=" + useResultSetAliases );
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows)
			throws PEException {
		throw new PECodingException(this.getClass().getSimpleName()+".inject not supported");
	}

    @Override
    public void writeCommandExecutor(Channel channel, StorageSite site, DBConnection.Monitor connectionMonitor, SQLCommand sql, CompletionHandle<Boolean> promise) {
		channel.write(new MysqlStmtExecuteCommand(sql, connectionMonitor, pstmt, sql.getParameters(), this, promise));
	}

	private RedistTupleBuilder getTargetHandler() throws PEException {
		try {
			if (targetHandler.get() == null) {
				if (logger.isDebugEnabled())
					logger.debug("About to call handlerFuture.sync(): " + handlerFuture);
                //SMG: sync called inside netty, not awesome.
				targetHandler.set(handlerFuture.sync());
			}
			return targetHandler.get();
		} catch (Exception e) {
			throw new PEException("Processing redist results", e);
		}
	}

	@Override
	public void setSenderCount(int senderCount) {
		this.senderCount = senderCount;
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

    @Override
    public void active(ChannelHandlerContext ctx) {
        //called when redist source is ready to start receiving packets.
        try {
            getTargetHandler().sourceActive(ctx);
        } catch (PEException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean emptyResultSet(MyOKResponse ok) throws PEException {
        senderCount--;
        if (senderCount == 0)
            getTargetHandler().setProcessingComplete();
        return ok.getAffectedRows() > 0;
    }

    public void error(MyErrorResponse errorResponse) throws PEException {
        Exception generatedError = new PEException(errorResponse.toString(), errorResponse.asException());
		try {
			getTargetHandler().failure(generatedError);
		} catch (Exception e) {
			throw new PEException("Processing redist results", e);
		}
	}

    @Override
    public void fieldCount(MyColumnCount colCount) {
        if (this.fieldCount == -1) {
            this.fieldCount = colCount.getColumnCount();
            resultColumnMetadata = new ColumnSet(fieldCount);
            for (int i = 0; i < fieldCount; ++i)
                resultColumnMetadata.addColumn(null);
//				System.out.println("Set column result " + resultColumnMetadata.size());
        }
	}

	@Override
	public void fieldEOF(MyMessage unknown)
			throws PEException {
        if (columnInspectorComputed == false) {
            columnInspectorComputed = true;
            getTargetHandler().setRowSetMetadata(tableHints.addAutoIncMetadata(resultColumnMetadata));

            int keyCount = 0;
            if (columnInspectorList == null) {
                columnInspectorList = new ArrayList<MysqlRedistTupleForwarder.ColumnValueInspector>();
                typeEncoders = new ArrayList<>();
                // if we don't have any distribution vector columns and auto incr values weren't specified
                // we can skip populating the ColumnValueInspector
                if ( !distValue.isEmpty() || tableHints.usesExistingAutoIncs() ) {
                    String colName;
                    boolean addMoreInspectors = true;
                    for (ColumnMetadata columnMetadata : resultColumnMetadata.getColumnList()) {
                        DataTypeValueFunc typeEncoder = DBTypeBasedUtils.getMysqlTypeFunc(columnMetadata);
                        typeEncoders.add(typeEncoder);
                        if (addMoreInspectors){
                            ColumnValueInspector dvm = new ColumnValueInspector();
                            dvm.typeReader = typeEncoder;
                            // if the caller ask to use aliases, still only use if alias set on the ColumnMetadata
                            colName = useResultSetAliases ?
                                    (columnMetadata.usingAlias() ? columnMetadata.getAliasName(): columnMetadata.getName())
                                    : columnMetadata.getName();
                                    if (distValue.containsKey(colName)) {
                                        dvm.dvCol = colName;
                                        ++keyCount;
                                    }
                                    dvm.isIncrCol = (tableHints.usesExistingAutoIncs() && tableHints.isExistingAutoIncColumn(columnMetadata.getOrderInTable()));
                                    columnInspectorList.add(dvm);

                                    if (logger.isDebugEnabled())
                                        logger.debug("inspecting field " + columnInspectorList.size() + " from " + columnMetadata + " using " + dvm.typeReader);
                                    if (keyCount == distValue.size() && (!tableHints.usesExistingAutoIncs() || (tableHints.usesExistingAutoIncs() && dvm.isIncrCol)))
                                        addMoreInspectors = false;
                        }
                    }
                    //					if ( keyCount != distValue.size() )
                    //					throw new PEException("Columns required for distribution vector not found in result set metadata. Expected "
                    //							+ distValue.size() + " found " + keyCount);
                }
            }
        }
	}
	
	@Override
	public void rowEOF(MyEOFPktResponse wholePacket)
			throws PEException {
        senderCount --;
		if (senderCount == 0)
			getTargetHandler().setProcessingComplete();
	}

    @Override
    public void rowBinary(MyBinaryResultRow binRow) throws PEException {
        int rowCount1 = 1;

        long[] autoIncrBlocks = null;
        MaximumAutoIncr maxAutoIncr = null;

        if (tableHints.tableHasAutoIncs())
            autoIncrBlocks = tableHints.buildBlocks(catalogDAO, rowCount1);
        if (tableHints.usesExistingAutoIncs())
            maxAutoIncr = new MaximumAutoIncr();

        MappingSolution mappingSolution;
        if ( BroadcastDistributionModel.SINGLETON.equals(distModel) && !tableHints.isUsingAutoIncColumn()) {
            mappingSolution = MappingSolution.AllWorkersSerialized;

            getTargetHandler().processSourcePacket(mappingSolution, binRow, fieldCount, resultColumnMetadata, autoIncrBlocks);
        } else {
            long nextAutoIncr = tableHints.tableHasAutoIncs() ? autoIncrBlocks[0] : 0;

            while (rowCount1-- > 0) {
                KeyValue dv = new KeyValue(distValue);
//                    int bitmapSize = MyNullBitmap.computeSize(fieldCount, BitmapType.RESULT_ROW);
//                    byte[] nullBitmap = MysqlAPIUtils.readBytes(resultRowPacket, bitmapSize);
//                    MyNullBitmap resultBitmap =  new MyNullBitmap(nullBitmap, fieldCount, BitmapType.RESULT_ROW);

                for (int i = 0; i < columnInspectorList.size(); ++i) {
                    ColumnValueInspector dvm = columnInspectorList.get(i);
                    dvm.inspectValue(binRow, i, dv, maxAutoIncr);
                }
                mappingSolution = distModel.mapKeyForInsert(catalogDAO, targetWG.getGroup(), dv);
//				if (logger.isDebugEnabled())
//					logger.debug("Redistribution maps dv " + dv + " to " + mappingSolution);

                long[] rowAutoIncrBlock = tableHints.tableHasAutoIncs() ? new long[] {nextAutoIncr++} : null;

                getTargetHandler().processSourcePacket(mappingSolution, binRow, fieldCount, resultColumnMetadata, rowAutoIncrBlock);
            }

            if (maxAutoIncr != null && maxAutoIncr.isSet())
                tableHints.recordMaximalAutoInc(catalogDAO, maxAutoIncr.getMaxValue());
        }
    }

    @Override
    public void rowText(MyTextResultRow textRow) throws PEException {
        throw new PECodingException("Didn't expect text results in " + this.getClass().getSimpleName());
    }

    @Override
    public void rowFlush() throws PEException {
        //ignored.
    }


    @Override
	public void field(int fieldIndex, MyFieldPktResponse columnDef, ColumnInfo columnProjection)
			throws PEException {

		if (this.fieldIndex == fieldIndex) {
            this.fieldIndex ++;
            ColumnMetadata columnMeta = FieldMetadataAdapter.buildMetadata(columnDef);
            columnMeta.setOrderInTable(fieldIndex);
			resultColumnMetadata.setColumn(fieldIndex, columnMeta);
//			System.out.println("Released permit");
		}
	}
	
	public int getNumRowsForwarded() {
		return rowCount;
	}

	@Override
	public void rollback() {
		rowCount = 0;
	}


}
