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
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.DBConnection;
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

    private CommandChannel channel;
    private int pstmtId = -1;
    private boolean waitingForPrepare = false;
    private BufferedExecute bufferedExecute = new BufferedExecute();

    private BufferedExecute pendingFlush = null;
    private int totalQueuedBytes = 0;
    private int totalQueuedRows = 0;
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

    public RedistTargetSite(RedistTupleBuilder builder, CommandChannel channel, InsertPolicy policy) {
        this.builder = builder;
        this.channel = channel;
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

    public void flush() {
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
                    MysqlStmtPrepareCommand prepareCmd = new MysqlStmtPrepareCommand(insertCommand.getSQL(), prepareCollector1, new PEDefaultPromise<Boolean>());

                    this.waitingForPrepare = true; //we flip this back when the prepare response comes back in.

                    //sends the prepare with the callback that will issue the execute.
//                    this.ctx.channel().writeAndFlush(prepareCmd);
                    this.channel.writeAndFlush(prepareCmd);
                } else {
                    //we have a valid statementID, so do the work now.
                    executePendingInsert();
                }

            } else {
                //TODO: If total queued rows <=0, decrease queued row count by buffers to flush?  Something about this smells bad. -sgossard
                this.queuedRowSetCount.addAndGet(-1 * buffersToFlush.size());
            }
        }
    }

    public boolean willAcceptMoreRows(){
        boolean channelBackedUp = (channel.isOpen() && !channel.isWritable());
        return !waitingForPrepare && !channelBackedUp;
    }

    private void executePendingInsert() {
        BufferedExecute buffersToFlush = pendingFlush;
        //OK, expectation here is that we always have a prepared statement for the appropriate number of tuples.
        int currentStatementID = this.pstmtId;

        buffersToFlush.setStmtID(currentStatementID);
        buffersToFlush.setNeedsNewParams(true); //TODO:this field can be managed by BufferedExecute. -gossard
        buffersToFlush.setRowSetMetadata(policy.getRowsetMetadata());
        buffersToFlush.setColumnsPerTuple(columnsPerTuple);

        int rowsWritten = buffersToFlush.size();

        this.channel.writeAndFlush(buffersToFlush, constructInsertHandler());
        this.pendingFlush = null;
        this.queuedRowSetCount.getAndAdd(-rowsWritten);

        // ********************

    }

    private MysqlCommandResultsProcessor constructInsertHandler() {
        return new MysqlCommandResultsProcessor() {
            @Override
            public void active(ChannelHandlerContext ctx) {
            }

            @Override
            public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
                return builder.processTargetPacket(RedistTargetSite.this,message);
            }

            @Override
            public void packetStall(ChannelHandlerContext ctx) throws PEException {
            }

            @Override
            public void failure(Exception e) {
                builder.failure(e);
            }
        };
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

            MSPComStmtCloseRequestMessage closeRequestMessage = MSPComStmtCloseRequestMessage.newMessage(this.pstmtId);
            this.channel.write(new SimpleMysqlCommandBundle(new SimpleRequestProcessor(closeRequestMessage,false,false), new NoopResultProcessor()));
            this.pstmtId = -1;
            this.pstmtTupleCount = -1;
        }
    }

    protected void prepareFinished(long stmtID, int tupleCount){
        this.waitingForPrepare = false;
        this.pstmtId = (int)stmtID;
        this.pstmtTupleCount = tupleCount;
    }

    protected void prepareFailed(MyErrorResponse error){
        this.waitingForPrepare = false;
        //TODO: need a better way to propigate this backwards. -gossard
        logger.error("prepare failed, error=" + error);
    }

    private class NoopResultProcessor implements MysqlCommandResultsProcessor {
        @Override
        public void active(ChannelHandlerContext ctx) {
        }

        @Override
        public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
            return false;
        }

        @Override
        public void packetStall(ChannelHandlerContext ctx) throws PEException {
        }

        @Override
        public void failure(Exception e) {
        }

    }
}
