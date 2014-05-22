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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.common.PECollectionUtils;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class RedistTupleBuilder implements MysqlMultiSiteCommandResultsProcessor {
	
	static Logger logger = Logger.getLogger(RedistTupleBuilder.class);
	
	static private AtomicInteger nextId = new AtomicInteger();
	private int thisId = nextId.incrementAndGet();


	class SiteContext {
		private static final int MAX_PACKET_PERMITS = 100;
		ChannelHandlerContext ctx;
		int pstmtId = -1;
        BufferedExecute bufferedExecute = new BufferedExecute();
		int totalQueuedBytes = 0;
		int totalQueuedRows = 0;
		boolean needsNewParam = true;
		AtomicReference<Future<Void>> flushFuture = new AtomicReference<Future<Void>>();
		ReentrantLock siteCtxLock = new ReentrantLock();
		Semaphore packetWritePermits = new Semaphore(MAX_PACKET_PERMITS);
		public int pstmtTupleCount = 0;
		AtomicInteger pendingStatementCount = new AtomicInteger();
		AtomicInteger queuedRowSetCount = new AtomicInteger();
	}
	Map<StorageSite, SiteContext> siteCtxBySite = new ConcurrentHashMap<StorageSite, SiteContext>();
	Map<Channel, SiteContext> siteCtxByChannel = new ConcurrentHashMap<Channel, SiteContext>();

	AtomicInteger updatedRowsCount = new AtomicInteger();
	PEPromise<Integer> completionPromise = new PEDefaultPromise<Integer>();
	
	ReentrantLock processPacketLock = new ReentrantLock();

	boolean lastPacketSent = false;

	final Future<SQLCommand> insertStatementFuture;
	final PersistentTable targetTable;
	final WorkerGroup targetWG;
	final PEPromise<RedistTupleBuilder> readyPromise;

	final int maximumRowCount;
	final int maxDataSize;
	final SQLCommand insertOptions;
	boolean insertIgnore = false;

	private ColumnSet rowSetMetadata;

	public RedistTupleBuilder(Future<SQLCommand> insertStatementFuture, SQLCommand insertOptions,
			PersistentTable targetTable, int maximumRowCount, int maxDataSize,
			PEPromise<RedistTupleBuilder> readyPromise,
			WorkerGroup targetWG) {
		this.insertOptions = insertOptions;
		this.insertStatementFuture = insertStatementFuture;
		this.targetTable = targetTable;
		this.readyPromise = readyPromise;
		this.targetWG = targetWG;
		this.maximumRowCount = maximumRowCount;
		this.maxDataSize = maxDataSize;
	}

	public void execute(MappingSolution mappingSolution,
			MyBinaryResultRow binRow, int fieldCount, ColumnSet columnSet, long[] autoIncrBlocks)
			throws PEException {

		if (mappingSolution == MappingSolution.AllWorkers || mappingSolution == MappingSolution.AllWorkersSerialized) {
			for (SiteContext siteCtx : siteCtxBySite.values())
				writePacket(binRow, (autoIncrBlocks == null) ? null : new long[] { autoIncrBlocks[0] }, siteCtx);
		} else if (mappingSolution == MappingSolution.AnyWorker || mappingSolution == MappingSolution.AnyWorkerSerialized) {
			writePacket(binRow, autoIncrBlocks, PECollectionUtils.selectRandom(siteCtxBySite.values()));
		} else {
			StorageSite executionSite = targetWG.resolveSite(mappingSolution.getSite());
			writePacket(binRow, autoIncrBlocks, siteCtxBySite.get(executionSite));
		}
	}

	/**
	 *
     * @param binRow
     * @param autoIncrBlocks
     * @param siteCtx
     * @throws PEException
	 */
	private void writePacket(MyBinaryResultRow binRow, long[] autoIncrBlocks, SiteContext siteCtx) throws PEException {
		siteCtx.siteCtxLock.lock();
		try {

			int rowsToFlushCount = 1;
			int bytesToFlushCount = binRow.sizeInBytes();


            boolean needsFlush = (siteCtx.totalQueuedRows + rowsToFlushCount >= maximumRowCount) || siteCtx.totalQueuedBytes + bytesToFlushCount >= maxDataSize;

            long[] autoIncUsed = autoIncrBlocks;
            if (needsFlush && autoIncrBlocks != null) {
                autoIncUsed = new long[] { autoIncrBlocks[0] };
                autoIncrBlocks[0] += rowsToFlushCount;
            }

            siteCtx.bufferedExecute.add(binRow, autoIncUsed);
            siteCtx.queuedRowSetCount.incrementAndGet();
            siteCtx.totalQueuedBytes += bytesToFlushCount;
            siteCtx.totalQueuedRows += rowsToFlushCount;

            if (needsFlush)
				submitFlush(siteCtx);

		} finally {
			siteCtx.siteCtxLock.unlock();
		}
	}

	private void submitFlush(final SiteContext siteCtx) throws PEException  {
        //NOTE: this code assumes we are holding the siteCtxLock mutex. -sgossard

		syncPreviousFlush(siteCtx.flushFuture.getAndSet(null));

		final BufferedExecute buffersToFlush = siteCtx.bufferedExecute;
		if (!buffersToFlush.isEmpty()) {
			final int rowsToFlush = siteCtx.totalQueuedRows; 
			siteCtx.bufferedExecute = new BufferedExecute();
			siteCtx.totalQueuedRows = 0;
			siteCtx.totalQueuedBytes = 0;
			if (rowsToFlush > 0) {
                siteCtx.flushFuture.set(Singletons.require(HostService.class).submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        flushBuffers(siteCtx, buffersToFlush);
                        return null;
                    }
                }));
			} else {
				siteCtx.queuedRowSetCount.addAndGet(-1 * buffersToFlush.size());
			}
		}
	}

	private void syncPreviousFlush(Future<Void> flushFuture) throws PEException {
		try {
			if (flushFuture != null)
				flushFuture.get();
		} catch (InterruptedException ie) {
			throw new PEException("Sync of previous redist buffer flush interrupted", ie);
		} catch (ExecutionException ee) {
			throw new PEException("Exception from redist buffer flush - check log for details", ee);
		}
	}

    private void flushBuffers(SiteContext siteCtx, BufferedExecute bufferedExecute) throws Exception {
        int columnsPerTuple = targetTable.getNumberOfColumns();

        int bufferedRowCount = bufferedExecute.size();
        int rowsToFlush = Math.min(bufferedRowCount, maximumRowCount);

        if (rowsToFlush != bufferedRowCount){
            throw new PECodingException(String.format("number of buffered rows, %s, exceeded the maximum row count of %s",bufferedRowCount,rowsToFlush));
        }

        if (siteCtx.pstmtId >= 0 && rowsToFlush != siteCtx.pstmtTupleCount) {
            closeActivePreparedStatement(siteCtx);
        }

        if (siteCtx.pstmtId < 0) {
            startNewPreparedStatement(siteCtx, rowsToFlush);
        }

        bufferedExecute.setStmtID(siteCtx.pstmtId);
        bufferedExecute.setNeedsNewParams(siteCtx.needsNewParam);
        bufferedExecute.setRowSetMetadata(rowSetMetadata);
        bufferedExecute.setColumnsPerTuple(columnsPerTuple);

        int rowsWritten = bufferedExecute.size();

        siteCtx.packetWritePermits.acquire();

        // ********************
        // order of these statements really matters.
        //
        // If a response comes in after we have queued all the input, we are done when no rows are queued or pending.
        // If we send the message out before incrementing the pending count, if pending was originally zero, a response
        // can come back on a different thread and think the redist is finished.
        // similarly if we decrement the queued row count before we increment the pending count, the queued rows can drop to
        // zero just before we increment pending, and another thread can catch things in the middle.
        //
        // The safest order of execution is increment pending, write the execute, then decrement the queued.

        siteCtx.pendingStatementCount.incrementAndGet();
        siteCtx.ctx.write(bufferedExecute);
        siteCtx.queuedRowSetCount.getAndAdd(-rowsWritten);
        siteCtx.ctx.flush();

        // ********************

        siteCtx.needsNewParam = false;
    }

    private void closeActivePreparedStatement(SiteContext siteCtx) {
		if (siteCtx.pstmtId >= 0) {
			// Close statement commands have no results from mysql, so we can just send the command directly on the 
			// channel context

            MSPComStmtCloseRequestMessage closeRequestMessage = MSPComStmtCloseRequestMessage.newMessage((byte) 0, siteCtx.pstmtId);
            siteCtx.ctx.write(closeRequestMessage);
			siteCtx.ctx.flush();
			siteCtx.pstmtId = -1;
			siteCtx.pstmtTupleCount = -1;
			siteCtx.needsNewParam = true;
		}
	}

	private void startNewPreparedStatement(SiteContext siteCtx, int tupleCount) throws Exception {
		SQLCommand insertCommand;
		if (tupleCount == maximumRowCount && insertStatementFuture != null) {
			try {
				insertCommand = insertStatementFuture.get();
			} catch (ExecutionException ee) {
				throw new PEException("Exception encountered syncing to redist insert statement", ee);
			} catch (InterruptedException ie) {
				throw new PEException("Sync to redist insert statement interrupted", ie);
			}
		} else {
			insertCommand = QueryStepMultiTupleRedistOperation.getTableInsertStatement(targetTable, insertOptions, rowSetMetadata, tupleCount, insertIgnore);
		}
		
		PEDefaultPromise<Boolean> preparePromise = new PEDefaultPromise<Boolean>();
		MysqlPrepareStatementCollector prepareCollector = new MysqlPrepareStatementCollector();
		prepareCollector.setExecuteImmediately(true);
		MysqlStmtPrepareCommand prepareCmd = new MysqlStmtPrepareCommand(insertCommand.getSQL(), prepareCollector, preparePromise);
		prepareCmd.setExecuteImmediately(true);
		
		siteCtx.packetWritePermits.acquire(SiteContext.MAX_PACKET_PERMITS);
		try {
			siteCtx.ctx.channel().write(prepareCmd);
			siteCtx.ctx.channel().flush();
		} finally {
			siteCtx.packetWritePermits.release(SiteContext.MAX_PACKET_PERMITS);
		}

		if (logger.isDebugEnabled())
			logger.debug("About to call preparePromise.sync(): " + preparePromise);
		preparePromise.sync(); // Waits until all of the results of the prepare statement have been processed
		
		siteCtx.pstmtId = prepareCollector.getPreparedStatement().getStmtId().getStmtId(siteCtx.ctx.channel());
		siteCtx.pstmtTupleCount = tupleCount;
	}

    @Override
    public boolean isDone(ChannelHandlerContext ctx){
        SiteContext siteCtx = siteCtxByChannel.get(ctx.channel());
        return isProcessingComplete(siteCtx);
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) {
    }

	@Override
	public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
		SiteContext siteCtx = siteCtxByChannel.get(ctx.channel());
		if (!isProcessingComplete(siteCtx)) {
			processPacketLock.lock();
			try {
				siteCtx.packetWritePermits.release();
				if (message instanceof MyOKResponse) {
					if (!isProcessingComplete(siteCtx) && !completionPromise.isFulfilled()) { // skip if previous exception
                        int rowCount = (int) ((MyOKResponse)message).getAffectedRows();
						updatedRowsCount.addAndGet(rowCount);
					}
				} else {
                    MyErrorResponse err = (MyErrorResponse)message;
					failure(err.asException());
				}
			} finally {
				siteCtx.pendingStatementCount.decrementAndGet();
				processPacketLock.unlock();
			}
		}

        testRedistributionComplete();
//		if (logger.isDebugEnabled())
//			logger.debug("RedistTupleBuilder.processPacket checks isProcessingComplete: lastPacketSent = " + lastPacketSent + ", pendingRowCount = " + pendingStatementCount.get());
//		if (isProcessingComplete())
//			completionPromise.trySuccess(updatedRowsCount.get());
		return isProcessingComplete(siteCtx);
	}

	private boolean isProcessingComplete(SiteContext siteCtx) {
		return (lastPacketSent && siteCtx.pendingStatementCount.get() == 0 && siteCtx.queuedRowSetCount.get() == 0);
	}
	
	public void setProcessingComplete() throws PEException {
		try {
			for (SiteContext siteContext : siteCtxBySite.values()) {
				siteContext.siteCtxLock.lock();
				try {
					submitFlush(siteContext);
				} finally {
					siteContext.siteCtxLock.unlock();
				}
			}

			for (SiteContext siteCtx : siteCtxBySite.values()) {
				siteCtx.siteCtxLock.lock();
				try {
					syncPreviousFlush(siteCtx.flushFuture.get());
					closeActivePreparedStatement(siteCtx);
				} catch (PEException e) {
					siteCtx.bufferedExecute = null;
					siteCtx.queuedRowSetCount.set(0);
					throw e;
				} finally {
					siteCtx.siteCtxLock.unlock();
				}
			}
			testRedistributionComplete();
		} catch (Exception e) {
			completionPromise.failure(e);
		}

		for (SiteContext siteCtx : siteCtxBySite.values()) {
			siteCtx.siteCtxLock.lock();
			try {
				syncPreviousFlush(siteCtx.flushFuture.get());
				closeActivePreparedStatement(siteCtx);
			} finally {
				siteCtx.siteCtxLock.unlock();
			}
		}
        lastPacketSent = true;
		testRedistributionComplete();
	}

	private void testRedistributionComplete() {
		boolean isProcessingComplete = lastPacketSent;
		for (SiteContext siteCtx : siteCtxBySite.values()) {
			if (siteCtx.pendingStatementCount.get() != 0 || siteCtx.queuedRowSetCount.get() != 0) {
				isProcessingComplete = false;
				break;
			}
		}
		if (isProcessingComplete) {
			if (logger.isDebugEnabled())
				logger.debug("Redistribution of " + targetTable.displayName() + " complete - " + updatedRowsCount + " rows updated");
			completionPromise.trySuccess(updatedRowsCount.get());
		}
	}

	@Override
	public void failure(Exception e) {
		completionPromise.failure(e);
	}

	public int getUpdateCount() throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("About to call completionPromise.sync(): " + completionPromise);
		return completionPromise.sync();
	}

	@Override
	public void addSite(StorageSite site, ChannelHandlerContext ctx) {
		SiteContext siteCtx = new SiteContext();
		siteCtx.ctx = ctx;
		siteCtxBySite.put(site, siteCtx);
		siteCtxByChannel.put(ctx.channel(), siteCtx);
		readyPromise.success(this);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName()+"{"+thisId+"}";
	}


	public void setRowSetMetadata(ColumnSet resultColumnMetadata) {
		this.rowSetMetadata = resultColumnMetadata;
	}

	public void setInsertIgnore(boolean insertIgnore) {
		this.insertIgnore = insertIgnore;
	}


}