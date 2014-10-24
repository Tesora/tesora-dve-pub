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
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.MSPComPrepareStmtRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.messaging.SQLCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
*
*/
public class RedistTargetSite implements AutoCloseable {
    static Logger logger = Logger.getLogger(RedistTargetSite.class);

    public interface InsertWatcher {
        void insertOK(RedistTargetSite site, MyOKResponse okPacket);
        void insertFailed(RedistTargetSite site, MyErrorResponse errorPacket);
        void insertFailed(RedistTargetSite site, Exception e);
    }

    public interface SourceControl {
        void pauseSourceStreams();
        void resumeSourceStreams();
    }

    private InsertWatcher watcher;

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
    private final long maximumBytesToBuffer;
    private final int columnsPerTuple;

    public interface InsertPolicy {
        int getMaximumRowsToBuffer();
        long getMaximumBytesToBuffer();
        int getColumnsPerTuple();
        SQLCommand buildInsertStatement(int tupleCount) throws PEException;
    }

    public RedistTargetSite(InsertWatcher watcher, CommandChannel channel, InsertPolicy policy) {
        this.watcher = watcher;
        this.channel = channel;
        this.policy = policy;

        this.maximumRowsToBuffer = policy.getMaximumRowsToBuffer();
        this.maximumBytesToBuffer = policy.getMaximumBytesToBuffer();
        this.columnsPerTuple = policy.getColumnsPerTuple();
    }

    public boolean append(MyBinaryResultRow binRow) {
        return append(binRow, 1,binRow.sizeInBytes());
    }

    public boolean append(MyBinaryResultRow binRow, int rowsToFlushCount, int bytesToFlushCount) {
        this.bufferedExecute.add(binRow);
        this.queuedRowSetCount.incrementAndGet();
        this.totalQueuedBytes += bytesToFlushCount;
        this.totalQueuedRows += rowsToFlushCount;

        boolean needsFlush = this.getTotalQueuedRows() >= maximumRowsToBuffer || this.getTotalQueuedBytes() >= maximumBytesToBuffer;
        if (needsFlush) {
            this.flush();
            return true;
        } else {
            return false;
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
                            prepareFailed(error);
                        }
                    };
                    MysqlMessage message = MSPComPrepareStmtRequestMessage.newMessage(insertCommand.getSQL(), this.channel.lookupCurrentConnectionCharset());
                    MysqlStmtPrepareCommand prepareCmd = new MysqlStmtPrepareCommand(this.channel,insertCommand.getSQL(), prepareCollector1, new PEDefaultPromise<Boolean>());

                    this.waitingForPrepare = true; //we flip this back when the prepare response comes back in.

                    //sends the prepare with the callback that will issue the execute.
//                    this.ctx.channel().writeAndFlush(prepareCmd);
                    this.channel.writeAndFlush(message,prepareCmd);
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
                if (message instanceof MyOKResponse){
                    RedistTargetSite.this.pendingStatementCount.decrementAndGet();
                    watcher.insertOK(RedistTargetSite.this,(MyOKResponse)message);
                } else if (message instanceof MyErrorResponse){
                    RedistTargetSite.this.pendingStatementCount.decrementAndGet();
                    watcher.insertFailed(RedistTargetSite.this, (MyErrorResponse) message);
                } else {
                    Exception weirdPacket = new PEException("Received unexpected packet," + (message == null? "null" : message.getClass().getSimpleName()));
                    watcher.insertFailed(RedistTargetSite.this, weirdPacket);
                }
                return false;
            }

            @Override
            public void packetStall(ChannelHandlerContext ctx) throws PEException {
            }

            @Override
            public void failure(Exception e) {
                watcher.insertFailed(RedistTargetSite.this, e);
            }

            @Override
            public void end(ChannelHandlerContext ctx) {

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
            this.channel.write( closeRequestMessage,NoopResponseProcessor.NOOP );
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
        watcher.insertFailed(this,error);
    }

}
