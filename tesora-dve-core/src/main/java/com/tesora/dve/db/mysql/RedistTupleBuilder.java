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

import com.tesora.dve.concurrent.*;
import com.tesora.dve.db.mysql.portal.protocol.StreamValve;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.common.PECollectionUtils;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class RedistTupleBuilder implements MysqlMultiSiteCommandResultsProcessor, RedistTargetSite.InsertPolicy {
    static final Logger logger = Logger.getLogger(RedistTupleBuilder.class);
    static final String SIMPLE_CLASSNAME = RedistTupleBuilder.class.getSimpleName();

	static private AtomicInteger nextId = new AtomicInteger();
	private int thisId = nextId.incrementAndGet();



    Map<StorageSite, RedistTargetSite> siteCtxBySite = new HashMap<>();
	Map<Channel, RedistTargetSite> siteCtxByChannel = new HashMap<>();

    IdentityHashMap<RedistTargetSite,RedistTargetSite> blockedTargetSites = new IdentityHashMap<>();
    IdentityHashMap<ChannelHandlerContext,ChannelHandlerContext> sourceSites = new IdentityHashMap<>();

	int updatedRowsCount = 0;

	final PEDefaultPromise<Integer> completionPromise = new PEDefaultPromise<>();

    boolean sourcePaused = false;

	boolean lastPacketSent = false;
    boolean failedRedist = false;

	final Future<SQLCommand> insertStatementFuture;
	final PersistentTable targetTable;
	final WorkerGroup targetWG;
	final PECountdownPromise<RedistTupleBuilder> readyCountdownPromise;

	final int maximumRowCount;
	final int maxDataSize;
	final SQLCommand insertOptions;
	boolean insertIgnore = false;

	private ColumnSet rowSetMetadata;

	public RedistTupleBuilder(Future<SQLCommand> insertStatementFuture, SQLCommand insertOptions,
			PersistentTable targetTable, int maximumRowCount, int maxDataSize,
			PECountdownPromise<RedistTupleBuilder> readyCountdownPromise,
			WorkerGroup targetWG) {
		this.insertOptions = insertOptions;
		this.insertStatementFuture = insertStatementFuture;
		this.targetTable = targetTable;
		this.readyCountdownPromise = readyCountdownPromise;
		this.targetWG = targetWG;
		this.maximumRowCount = maximumRowCount;
		this.maxDataSize = maxDataSize;
	}

	public void processSourcePacket(MappingSolution mappingSolution, MyBinaryResultRow binRow, int fieldCount, ColumnSet columnSet, long[] autoIncrBlocks)
			throws PEException {

        if (failedRedist)
            return;//throw out source rows if redist has failed.

		if (mappingSolution == MappingSolution.AllWorkers || mappingSolution == MappingSolution.AllWorkersSerialized) {
			for (RedistTargetSite siteCtx : siteCtxBySite.values())
				handleSourceRow(binRow, (autoIncrBlocks == null) ? null : new long[]{autoIncrBlocks[0]}, siteCtx);
		} else if (mappingSolution == MappingSolution.AnyWorker || mappingSolution == MappingSolution.AnyWorkerSerialized) {
			handleSourceRow(binRow, autoIncrBlocks, PECollectionUtils.selectRandom(siteCtxBySite.values()));
		} else {
			StorageSite executionSite = targetWG.resolveSite(mappingSolution.getSite());
			handleSourceRow(binRow, autoIncrBlocks, siteCtxBySite.get(executionSite));
		}

        if (!blockedTargetSites.isEmpty()){
            pauseSourceStreams();
        }

	}

    public void setProcessingComplete() throws PEException {
        //Called when upstream forwarder has seen stream EOFs from all source streams, so all rows have been forwarded.

        //unpause all source streams so they are ready for re-use.
        resumeSourceStreams();

        lastPacketSent = true;
        flushTargetSites();

        testRedistributionComplete();
    }

    @Override
    public void active(ChannelHandlerContext ctx) {
        //called when the RedistTupleBuilder is ready to receive responses on the target sockets.
        this.targetActive(ctx);
    }

    @Override
    public void failure(Exception e) {
        failedRedist = true;
        completionPromise.failure(e);
    }

    @Override
    public void addSite(StorageSite site, ChannelHandlerContext ctx) {
        addTargetSite(site, ctx);
    }

    public int getUpdateCount() throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("redist # "+thisId+" , about to call completionPromise.sync(): " + completionPromise);
        return completionPromise.sync();
    }

    @Override
    public boolean isDone(ChannelHandlerContext ctx){
        RedistTargetSite siteCtx = siteCtxByChannel.get(ctx.channel());

        return isProcessingComplete(siteCtx);
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) {
        targetPacketStall(ctx);
    }

    @Override
    public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
        return processTargetPacket(ctx, message);
    }

    @Override
    public int getMaximumRowsToBuffer() {
        return maximumRowCount;
    }

    @Override
    public int getColumnsPerTuple() {
        return targetTable.getNumberOfColumns();
    }

    @Override
    public ColumnSet getRowsetMetadata() {
        return rowSetMetadata;
    }

    public SQLCommand buildInsertStatement(int tupleCount) throws PEException {
        SQLCommand insertCommand;
        if (tupleCount == maximumRowCount && insertStatementFuture != null) {
            try {
                //TODO: this delayed building is suspect, we don't want to block a netty thread, and it ignores the tuple count. -sgossard
                insertCommand = insertStatementFuture.get();
            } catch (ExecutionException ee) {
                throw new PEException("Exception encountered syncing to redist insert statement", ee);
            } catch (InterruptedException ie) {
                throw new PEException("Sync to redist insert statement interrupted", ie);
            }
        } else {
            insertCommand = QueryStepMultiTupleRedistOperation.getTableInsertStatement(targetTable, insertOptions, rowSetMetadata, tupleCount, insertIgnore);
        }
        return insertCommand;
    }


    public void setRowSetMetadata(ColumnSet resultColumnMetadata) {
        this.rowSetMetadata = resultColumnMetadata;
    }

    public void setInsertIgnore(boolean insertIgnore) {
        this.insertIgnore = insertIgnore;
    }

    @Override
    public String toString() {
        return SIMPLE_CLASSNAME + "{" + thisId + "}";
    }



    protected void targetActive(ChannelHandlerContext ctx){
        //NOOP.   No harm in having this here, the JIT will eliminate it.
    }

    public void sourceActive(ChannelHandlerContext ctx){
        //this is called by MysqlRedistTupleForwarder instances that are processing the source queries when they are in position to receive response packets.
        if (sourceSites.get(ctx) == null){
            sourceSites.put(ctx,ctx);

            //new site, pause it if we are paused.
            if (sourcePaused) {
                StreamValve.pipelinePause(ctx.pipeline());
            }
        }
    }

    protected void pauseSourceStreams(){
        sourcePaused = true;
        for (ChannelHandlerContext ctx : sourceSites.keySet()){
            StreamValve.pipelinePause(ctx.pipeline());
        }
    }

    protected void resumeSourceStreams(){
        if (sourcePaused){
            sourcePaused = false;
            for (ChannelHandlerContext ctx : sourceSites.keySet()){
                StreamValve.pipelineResume(ctx.pipeline());
            }
        }
    }

	/**
	 *
     * @param binRow
     * @param autoIncrBlocks
     * @param siteCtx
     * @throws PEException
	 */
	protected void handleSourceRow(MyBinaryResultRow binRow, long[] autoIncrBlocks, RedistTargetSite siteCtx) throws PEException {
//		siteCtx.siteCtxLock.lock();
		try {

			int rowsToFlushCount = 1;
			int bytesToFlushCount = binRow.sizeInBytes();


            boolean needsFlush = (siteCtx.getTotalQueuedRows() + rowsToFlushCount >= maximumRowCount) || siteCtx.getTotalQueuedBytes() + bytesToFlushCount >= maxDataSize;

            long[] autoIncUsed = autoIncrBlocks;
            if (needsFlush && autoIncrBlocks != null) {
                autoIncUsed = new long[] { autoIncrBlocks[0] };
                autoIncrBlocks[0] += rowsToFlushCount;
            }

            siteCtx.append(binRow, rowsToFlushCount, bytesToFlushCount, autoIncUsed);

            if (siteCtx.getTotalQueuedRows() >= maximumRowCount || siteCtx.getTotalQueuedBytes() >= maxDataSize){
                siteCtx.flush();
                if (! siteCtx.willAcceptMoreRows() ){
                    blockedTargetSites.put(siteCtx,siteCtx);
                }
            }

        } finally {
//			siteCtx.siteCtxLock.unlock();
		}
	}

    private void targetPacketStall(ChannelHandlerContext ctx) {
        //NOOP.  just here
    }

    private boolean processTargetPacket(ChannelHandlerContext ctx, MyMessage message) {
        RedistTargetSite siteCtx = siteCtxByChannel.get(ctx.channel());

        if (!isProcessingComplete(siteCtx)) {
            try {
                if (message instanceof MyOKResponse) {
                    if (!isProcessingComplete(siteCtx) && !completionPromise.isFulfilled()) { // skip if previous exception
                        int rowCount = (int) ((MyOKResponse)message).getAffectedRows();
                        updatedRowsCount+= rowCount;
                    }
                } else {
                    MyErrorResponse err = (MyErrorResponse)message;
                    failure(err.asException());
                }
            } finally {
                siteCtx.handleAck(message);
                checkIfSitesAreUnblocked();
            }
        }

        testRedistributionComplete();

        return isProcessingComplete(siteCtx);
    }

    private void checkIfSitesAreUnblocked(){
        Iterator<RedistTargetSite> blockedSites = blockedTargetSites.keySet().iterator();
        while (blockedSites.hasNext()){
            RedistTargetSite site = blockedSites.next();
            if (site.willAcceptMoreRows())
                blockedSites.remove();
        }
        if (blockedTargetSites.isEmpty()) {
            resumeSourceStreams();
        }
    }

    private boolean isProcessingComplete(RedistTargetSite siteCtx) {
        boolean done = lastPacketSent && !siteCtx.hasPendingRows();

        return done;
	}
	


    private void closeTargetSites() {

        for (RedistTargetSite siteCtx : siteCtxBySite.values()) {
            siteCtx.close();
        }
    }

    private void flushTargetSites() {
        for (RedistTargetSite siteContext : siteCtxBySite.values()) {
            siteContext.flush();
        }
    }

    private void testRedistributionComplete() {
        boolean isProcessingComplete = lastPacketSent;
		for (RedistTargetSite siteCtx : siteCtxBySite.values()) {
			if ( siteCtx.hasPendingRows() ) {
				isProcessingComplete = false;
				break;
			}
		}

		if (isProcessingComplete) {
			if (logger.isDebugEnabled())
				logger.debug("redist # "+thisId+" , redistribution of " + targetTable.displayName() + " complete - " + updatedRowsCount + " rows updated");
            try{
                closeTargetSites();
                completionPromise.trySuccess(updatedRowsCount);
            } catch (Exception e){
                completionPromise.failure(e);
            }

		}
	}

    private void addTargetSite(StorageSite site, ChannelHandlerContext ctx) {
        RedistTargetSite siteCtx = new RedistTargetSite(this,ctx,this);
        siteCtxBySite.put(site, siteCtx);
        siteCtxByChannel.put(ctx.channel(), siteCtx);

        //not super obvious, but this is a countdown promise, and requires one success for each site before triggering the OK.
        readyCountdownPromise.success(this);
    }


}