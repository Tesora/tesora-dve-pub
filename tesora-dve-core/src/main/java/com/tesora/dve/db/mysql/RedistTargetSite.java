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

import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.messaging.SQLCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

/**
*
*/
class RedistTargetSite implements AutoCloseable {
    static Logger logger = Logger.getLogger(RedistTargetSite.class);

    private RedistTupleBuilder builder;

    private ChannelHandlerContext ctx;
    private int pstmtId = -1;
    private BufferedExecute bufferedExecute = new BufferedExecute();

    private BufferedExecute pendingFlush = null;
    private int totalQueuedBytes = 0;
    private int totalQueuedRows = 0;
    private boolean needsNewParam = true;
    private int pstmtTupleCount = 0;
    private AtomicInteger pendingStatementCount = new AtomicInteger();
    private AtomicInteger queuedRowSetCount = new AtomicInteger();

    private InsertPolicy policy;
    private final int maximumRowsToBuffer;
    private final int columnsPerTuple;

    public interface InsertPolicy {
        int getMaximumRowsToBuffer();
        int getColumnsPerTuple();
        ColumnSet getRowsetMetadata();
        SQLCommand buildInsertStatement(int tupleCount) throws PEException;
    }

    public RedistTargetSite(RedistTupleBuilder builder, ChannelHandlerContext ctx, InsertPolicy policy) {
        this.builder = builder;
        this.ctx = ctx;
        this.policy = policy;

        this.maximumRowsToBuffer = policy.getMaximumRowsToBuffer();
        this.columnsPerTuple = policy.getColumnsPerTuple();
    }

    public void append(MyBinaryResultRow binRow, int rowsToFlushCount, int bytesToFlushCount, long[] autoIncUsed) {
        this.bufferedExecute.add(binRow, autoIncUsed);
        this.queuedRowSetCount.incrementAndGet();
        this.totalQueuedBytes += bytesToFlushCount;
        this.totalQueuedRows += rowsToFlushCount;
    }

    public void handleAck(MyMessage message) {
        if (message instanceof MyOKResponse){
            this.pendingStatementCount.decrementAndGet();
        } else if (message instanceof MyErrorResponse){
            this.pendingStatementCount.decrementAndGet();
        } else {
            //?
        }
    }

    public boolean flush() {
        final BufferedExecute buffersToFlush = this.bufferedExecute;
        if (!buffersToFlush.isEmpty()) {
            final int rowsToFlush = this.totalQueuedRows;
            this.pendingFlush = this.bufferedExecute;
            this.bufferedExecute = new BufferedExecute();
            this.totalQueuedRows = 0;
            this.totalQueuedBytes = 0;
            if (rowsToFlush > 0) {

                this.pendingStatementCount.incrementAndGet();

                int bufferedRowCount = buffersToFlush.size();

                final int flushTupleCount = Math.min(bufferedRowCount, maximumRowsToBuffer);

                if (flushTupleCount != bufferedRowCount){
                    throw new PECodingException(String.format("number of buffered rows, %s, exceeded the maximum row count of %s",bufferedRowCount, flushTupleCount));
                }

                if (this.pstmtId >= 0 && flushTupleCount != this.pstmtTupleCount) {
                    //we have a prepared statement, but for the wrong tuple count.
                    this.closeActivePreparedStatement();
                }

                if (RedistTargetSite.this.pstmtId < 0) {
                    //we need a prepared statement.

                    SQLCommand insertCommand= null;
                    try {
                        insertCommand = policy.buildInsertStatement(flushTupleCount);
                    } catch (PEException e) {
                        throw new RuntimeException(e);
                    }

                    MysqlPrepareStatementCollector prepareCollector1 = new MysqlPrepareStatementCollector(){
                        long stmtID;
                        int paramCount;
                        int columnCount;

                        @Override
                        void consumeHeader(MyPrepareOKResponse prepareOK) {
                            this.stmtID = prepareOK.getStmtId();
                            this.paramCount = prepareOK.getNumParams();
                            this.columnCount =prepareOK.getNumColumns();
                            super.consumeHeader(prepareOK);
                            if (this.paramCount == 0 && this.columnCount == 0){
                                prepareFinished(stmtID, flushTupleCount);
                                executePendingInsert();
                            }
                        }


                        @Override
                        void consumeParamDefEOF(MyEOFPktResponse myEof) {
                            super.consumeParamDefEOF(myEof);
                            if (this.columnCount == 0){
                                prepareFinished(stmtID, flushTupleCount);
                                executePendingInsert();
                            }
                        }

                        @Override
                        void consumeColDefEOF(MyEOFPktResponse colEof) {
                            super.consumeColDefEOF(colEof);
                            prepareFinished(stmtID, flushTupleCount);
                            executePendingInsert();
                        }

                        @Override
                        void consumeError(MyErrorResponse error) throws PESQLStateException {
                            super.consumeError(error);
                            prepareFailed(error);
                        }
                    };
                    prepareCollector1.setExecuteImmediately(true);
                    MysqlStmtPrepareCommand prepareCmd = new MysqlStmtPrepareCommand(insertCommand.getSQL(), prepareCollector1, new PEDefaultPromise<Boolean>());
                    //SMG:remove
                    prepareCmd.setExecuteImmediately(true);

                    //sends the prepare with the callback that will issue the execute.
                    this.ctx.channel().writeAndFlush(prepareCmd);
                } else {
                    //we have a valid statementID, so do the work now.
                    executePendingInsert();
                }

            } else {
                //TODO: If total queued rows <=0, decrease queued row count by buffers to flush?  Something about this smells bad. -sgossard
                this.queuedRowSetCount.addAndGet(-1 * buffersToFlush.size());
            }
        }
        return allWritesFlushed();
    }

    public boolean allWritesFlushed(){
        return this.bufferedExecute.isEmpty() && this.pendingFlush == null;
    }

    private void executePendingInsert() {
        BufferedExecute buffersToFlush = pendingFlush;
        //OK, expectation here is that we always have a prepared statement for the appropriate number of tuples.
        int currentStatementID = this.pstmtId;

        buffersToFlush.setStmtID(currentStatementID);
        buffersToFlush.setNeedsNewParams(this.needsNewParam);
        buffersToFlush.setRowSetMetadata(policy.getRowsetMetadata());
        buffersToFlush.setColumnsPerTuple(columnsPerTuple);

        int rowsWritten = buffersToFlush.size();

//        this.ctx.writeAndFlush(buffersToFlush);
        this.ctx.channel().writeAndFlush(new WrappedExecuteCommand(buffersToFlush,builder));
        this.pendingFlush = null;
        this.queuedRowSetCount.getAndAdd(-rowsWritten);

        // ********************

        this.needsNewParam = false;
    }

    public int getTotalQueuedRows(){
        return totalQueuedRows;
    }

    public int getTotalQueuedBytes(){
        return totalQueuedBytes;
    }

    public boolean hasPendingRows(){
        return pendingStatementCount.get() > 0 || queuedRowSetCount.get() > 0;
    }

    @Override
    public void close(){
        try {
            this.closeActivePreparedStatement();
        } finally {
            ReferenceCountUtil.release(this.bufferedExecute);
            this.bufferedExecute = null;
            this.queuedRowSetCount.set(0);
        }
    }

    private void closeActivePreparedStatement() {
        if (this.pstmtId >= 0) {
            // Close statement commands have no results from mysql, so we can just send the command directly on the channel context

            MSPComStmtCloseRequestMessage closeRequestMessage = MSPComStmtCloseRequestMessage.newMessage((byte) 0, this.pstmtId);
            this.ctx.writeAndFlush(closeRequestMessage);
            this.pstmtId = -1;
            this.pstmtTupleCount = -1;
            this.needsNewParam = true;
        }
    }

    protected void prepareFinished(long stmtID, int tupleCount){
        this.pstmtId = (int)stmtID;
        this.pstmtTupleCount = tupleCount;
    }

    protected void prepareFailed(MyErrorResponse error){
        //SMG:debug
        System.out.println("***Prepare failed, "+error);
        //SMG: need a better way to propigate this backwards.
        logger.error("prepare failed, error=" + error);
    }

    public class WrappedExecuteCommand extends MysqlCommand implements MysqlCommandResultsProcessor {
        MyMessage executeMessage;
        RedistTupleBuilder builder;
        boolean receivedResponseOrError = false;

        public WrappedExecuteCommand(MyMessage executeMessage, RedistTupleBuilder builder) {
            this.executeMessage = executeMessage;
            this.builder = builder;
        }

        @Override
        void execute(ChannelHandlerContext ctx, Charset charset) throws PEException {
            ctx.writeAndFlush(executeMessage);
        }

        @Override
        MysqlCommandResultsProcessor getResultHandler() {
            return this;
        }

        public void active(ChannelHandlerContext ctx){
            builder.active(ctx);
        }

        public boolean isDone(ChannelHandlerContext ctx){
            builder.isDone(ctx); //in case builder had side effects.

            return receivedResponseOrError;
        }

        public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
            receivedResponseOrError = true;
            builder.processPacket(ctx,message);
            return false;
        }


        public void packetStall(ChannelHandlerContext ctx) throws PEException{
            builder.packetStall(ctx);
        }

        public void failure(Exception e){
            receivedResponseOrError = true;

            builder.failure(e);
        }


        @Override
        public boolean isExecuteImmediately() {
            return true;
        }

        @Override
        public boolean isPreemptable() {
            return false;
        }
    }

}
