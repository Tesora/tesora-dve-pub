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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.concurrent.*;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.portal.protocol.FlowControl;
import com.tesora.dve.db.mysql.portal.protocol.ValveFlowControlSet;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.queryplan.TableHints;
import com.tesora.dve.worker.MysqlRedistTupleForwarder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class RedistTupleBuilder implements RedistTargetSite.InsertPolicy, RedistTargetSite.InsertWatcher, FlowControl {
    static final Logger logger = Logger.getLogger(RedistTupleBuilder.class);
    static final String SIMPLE_CLASSNAME = RedistTupleBuilder.class.getSimpleName();

	static private AtomicInteger nextId = new AtomicInteger();
	private int thisId = nextId.incrementAndGet();

    RedistTargetSet targetSet;

    ValveFlowControlSet upstreamSet = new ValveFlowControlSet();

    class CountHolder {
        int sourceRows = 0;
        int updatedRows = 0;
        long sourceBytesTotal = 0;
    }

	final PEDefaultPromise<CountHolder> completionPromise = new PEDefaultPromise<>();

    CountHolder counts = new CountHolder();

	boolean lastPacketSent = false;
    boolean failedRedist = false;

	final PersistentTable targetTable;
	final WorkerGroup targetWG;
    final CatalogDAO catalogDAO;
    final DistributionModel distModel;

	final int maximumRowCount;
	final int maxDataSize;
	final SQLCommand insertOptions;
	boolean insertIgnore = false;

    TableHints tableHints;
    MysqlRedistTupleForwarder.MaximumAutoIncr maxAutoIncr = null;
	private ColumnSet rowSetMetadata;

	public RedistTupleBuilder(CatalogDAO catalogDAO, DistributionModel distModel, SQLCommand insertOptions,
                              PersistentTable targetTable, int maximumRowCount, int maxDataSize,
                              WorkerGroup targetWG) throws PEException {
        this.distModel = distModel;
        this.catalogDAO = catalogDAO;
		this.insertOptions = insertOptions;
		this.targetTable = targetTable;
		this.targetWG = targetWG;
		this.maximumRowCount = maximumRowCount;
		this.maxDataSize = maxDataSize;

        this.targetSet = new RedistTargetSet(targetWG,this,this,this);
	}


    public void processSourceRow(KeyValue distValue, List<MysqlRedistTupleForwarder.ColumnValueInspector> columnInspectorList, MyBinaryResultRow binRow) throws PEException {
        //this get's called by MysqlRedistTupleForwarder when we receive a new row from a source query.
        try {
            innerHandleRow(distValue, columnInspectorList, binRow);
        } catch (Exception e){
            this.failure(e);
        }
    }

    private void innerHandleRow(KeyValue distValue, List<MysqlRedistTupleForwarder.ColumnValueInspector> columnInspectorList, MyBinaryResultRow binRow) throws PEException {
        if (failedRedist)
            return;//drop our source rows quickly if redist has already failed.

        long[] autoIncrBlocks = null;

        if (tableHints.tableHasAutoIncs()) {
            //TODO: this call fetches a single autoinc, which might block on hibernate if the background pre-fetch is slow, stalling the netty thread.  sgossard
            autoIncrBlocks = tableHints.buildBlocks(catalogDAO, 1 /*rowcount*/); //gets an autoinc for one row.
        }

        if (tableHints.usesExistingAutoIncs() && maxAutoIncr == null)
            maxAutoIncr = new MysqlRedistTupleForwarder.MaximumAutoIncr();

        MappingSolution mappingSolution;
        if (BroadcastDistributionModel.SINGLETON.equals(distModel) && !tableHints.isUsingAutoIncColumn()) {
            //broadcast and not using an autoinc.
            mappingSolution = MappingSolution.AllWorkersSerialized;
        } else {
            //either using an autoinc, or not broadcast.

            KeyValue dv = new KeyValue(distValue);

            //picks apart the row, looking for distribution keys or autoinc fields.
            for (int i = 0; i < columnInspectorList.size(); ++i) {
                MysqlRedistTupleForwarder.ColumnValueInspector dvm = columnInspectorList.get(i);
                dvm.inspectValue(binRow, i, dv, maxAutoIncr);
            }

            // don't need exec state here - values are fully materialized
            mappingSolution = distModel.mapKeyForInsert(catalogDAO, targetWG.getGroup(), dv);
        }

        counts.sourceRows += 1;
        counts.sourceBytesTotal += binRow.sizeInBytes();

        //********************
        Long actualAutoInc = autoIncrBlocks == null ? null : autoIncrBlocks[0];
        if (actualAutoInc != null) {
            //we have an autoinc, append it to the binrow
            DecodedMeta autoIncFunc = new DecodedMeta(DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.FIELD_TYPE_LONGLONG));
            binRow = binRow.append(autoIncFunc, actualAutoInc);
        }



        boolean triggeredFlush = targetSet.sendInsert(mappingSolution, binRow);
        //********************

        //TODO: this should really be called BEFORE flushes, to ensure we don't lose a tracked autoinc on a failure. -sgossard
        if (triggeredFlush)
            updateAutoIncIfNeeded();
    }

    private void updateAutoIncIfNeeded() {
        if (maxAutoIncr != null && maxAutoIncr.isSet() && tableHints.isUsingAutoIncColumn()){
            //TODO: this call records the maximum autoinc via hibernate, and could stall the netty thread.  sgossard
            tableHints.recordMaximalAutoInc(catalogDAO, maxAutoIncr.getMaxValue());
            maxAutoIncr = null;
        }
    }

    public void setProcessingComplete() throws PEException {
        //Called when upstream forwarder has seen stream EOFs from all source streams, so all rows have been forwarded.

        //unpause all source streams so they are ready for re-use.
        resumeSourceStreams();

        lastPacketSent = true;

        updateAutoIncIfNeeded();

        targetSet.flush();

        testRedistributionComplete();
    }

    public void failure(Exception e) {
        failedRedist = true;
        completionPromise.failure(e);
    }

    public int getUpdateCount() throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("redist # "+thisId+" , about to call completionPromise.sync(): " + completionPromise);
        CountHolder holder = SynchronousListener.sync(completionPromise);
        return holder.updatedRows;
    }

    public int getSourceRowCount() throws Exception {
        CountHolder holder = SynchronousListener.sync(completionPromise);
        return holder.sourceRows;
    }

    public long getSourceRowBytesTotal() throws Exception {
        CountHolder holder = SynchronousListener.sync(completionPromise);
        return holder.sourceBytesTotal;
    }

    @Override
    public int getMaximumRowsToBuffer() {
        return maximumRowCount;
    }

    @Override
    public long getMaximumBytesToBuffer(){
        return maxDataSize;
    }

    @Override
    public int getColumnsPerTuple() {
        return targetTable.getNumberOfColumns();
    }

	public SQLCommand buildInsertStatement(int tupleCount) throws PEException {
        return QueryStepMultiTupleRedistOperation.getTableInsertStatement(
					/*
					 * PerHostConnectionManager.INSTANCE.lookupConnection(targetWG
					 * .getCommectionId())
					 */CharsetUtil.UTF_8, targetTable, insertOptions, rowSetMetadata, tupleCount,
					insertIgnore);
    }


    public void setRowSetMetadata(TableHints tableHints, ColumnSet resultColumnMetadata) {
        this.tableHints = tableHints;
        this.rowSetMetadata = resultColumnMetadata;
    }

    public void setInsertIgnore(boolean insertIgnore) {
        this.insertIgnore = insertIgnore;
    }

    @Override
    public String toString() {
        return SIMPLE_CLASSNAME + "{" + thisId + "}";
    }


    public void sourceActive(ChannelHandlerContext ctx){
        upstreamSet.register(ctx);
    }

    @Override
    public void pauseSourceStreams(){
        if (!(lastPacketSent || failedRedist) ) //don't pause source sockets if we are finished.
            upstreamSet.pauseSourceStreams();
    }

    @Override
    public void resumeSourceStreams(){
        upstreamSet.resumeSourceStreams();
    }


    @Override
    public void insertOK(RedistTargetSite siteCtx, MyOKResponse okPacket) {
        testRedistributionComplete();
    }

    @Override
    public void insertFailed(RedistTargetSite site, MyErrorResponse errorPacket) {
        this.failure( errorPacket.asException() );
    }

    @Override
    public void insertFailed(RedistTargetSite site, Exception e) {
        this.failure( e );
    }


    private void testRedistributionComplete() {
        boolean anySiteHasPending = targetSet.hasPendingRows();

        boolean isProcessingComplete = lastPacketSent && !anySiteHasPending;
		if (isProcessingComplete) {

            counts.updatedRows = (int) targetSet.getUpdatedRowCount();

            if (logger.isDebugEnabled()) {
                String formatted = String.format("redist # %s, redistribution of %s complete - source=%s, updated=%s, sourceBytes=%s", thisId, targetTable.displayName(), counts.sourceRows, counts.updatedRows, counts.sourceBytesTotal);
                logger.debug(formatted);
            }
            try{
                targetSet.close();
                completionPromise.trySuccess(counts);
            } catch (Exception e){
                completionPromise.failure(e);
            }

		}
	}

}