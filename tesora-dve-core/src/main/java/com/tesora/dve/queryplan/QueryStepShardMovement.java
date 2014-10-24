package com.tesora.dve.queryplan;

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

import com.tesora.dve.common.catalog.*;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.CompletionTarget;
import com.tesora.dve.db.*;
import com.tesora.dve.db.mysql.*;
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtExecuteRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.StreamValve;
import com.tesora.dve.distribution.*;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.ddl.AddStorageGenRangeInfo;
import com.tesora.dve.sql.statement.ddl.AddStorageGenRangeTableInfo;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class QueryStepShardMovement extends QueryStepOperation {
    //TODO: not sure if QueryStepOperation is the proper class to extend. -sgossard

    StorageGroupGeneration oldGen;
    DistributionRange range;
    PersistentSite oldSite;
    StorageGroupGeneration targetGen;
    AddStorageGenRangeInfo rebalanceInfo;

    public QueryStepShardMovement(StorageGroupGeneration oldGen, DistributionRange range, PersistentSite oldSite, StorageGroupGeneration targetGen, AddStorageGenRangeInfo rebalanceEntry) {
        //TODO: The source rows are scoped to all affected tables in (range, old generation, one site), and the target is a (new generation).  Ideally construction should reflect this.  -sgossard
        this.oldGen = oldGen;
        this.range = range;
        this.oldSite = oldSite;
        this.targetGen = targetGen;
        this.rebalanceInfo = rebalanceEntry;
    }

    public String toString(){
        TreeSet<String> tableNames = new TreeSet<>();
        for (AddStorageGenRangeTableInfo table : rebalanceInfo.getTableInfos()) {
            tableNames.add(table.getName().toString());
        }

        TreeSet<String> newSiteNames = new TreeSet<>();
        for (PersistentSite newSite : targetGen.getStorageSites()) {
            if (oldSite.equals(newSite))
                newSiteNames.add("<"+newSite.getName()+">");
            else
                newSiteNames.add(newSite.getName());
        }

        return String.format("shard movement: oldgen=[%s], newgen=[%s], range=[%s], tables=%s , from=[%s], to=%s", oldGen.getVersion(),targetGen.getVersion(),range.getName(),tableNames,oldSite.getName(),newSiteNames);
    }

    @Override
    public void execute(final SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
        System.out.printf("SMG: QueryStepShardMovement.execute() ==> %s\n",this.toString());

        //NOTE: currently assumes someone above us is dealing with the XA and catalog updates.
        //NOTE: does not return until all data for all affected tables in the range have been moved.  In the future, can be changed to be an incremental reduction of a range boundary.
        try {
            //TODO: this requires turning off fk checks, autocommit, probably needs some locking.

            Set<PersistentSite> sourceSite = Collections.singleton(oldSite);

            //provided wg has all old sites but no new sites, so we actually need to create three wgs.  one for all target sites, and two with just the single source site.

            PersistentGroup sourceGroup = new PersistentGroup(sourceSite);
            PersistentGroup targetGroup = new PersistentGroup(targetGen.getStorageSites());
            final WorkerGroup targetWG = WorkerGroup.WorkerGroupFactory.newInstance(ssCon, targetGroup, null);

            WorkerGroup sourceWG = WorkerGroup.WorkerGroupFactory.newInstance(ssCon, sourceGroup, null);
            final WorkerGroup sourceTempWG = sourceWG.clone(ssCon);

            Collection<Worker> deleteWorkers = sourceTempWG.getTargetWorkers(WorkerGroup.MappingSolution.AllWorkers);
            Worker deleteWorker = deleteWorkers.iterator().next(); //should only be one, by construction.
            final CommandChannel deleteChannel = deleteWorker.getDirectChannel();

            Database sideDatabase = null;
            if ( rebalanceInfo.hasSharedTable() ) {
                TempTable tempTable = rebalanceInfo.getSharedTable();
                sideDatabase = tempTable.getDatabase(ssCon.getSchemaContext());
                SQLCommand createSideTable = rebalanceInfo.getCreateSharedTable();
                System.out.printf("SMG:\tsql to create shared side table, %s\n",createSideTable.getRawSQL());
                WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), createSideTable).onDatabase(sideDatabase).forDDL();
                sourceTempWG.execute(WorkerGroup.MappingSolution.AllWorkers,req, DBEmptyTextResultConsumer.INSTANCE);
            }


            for (final AddStorageGenRangeTableInfo sourceTableInfo : rebalanceInfo.getTableInfos() ) {

                if ( ! rebalanceInfo.hasSharedTable() ) {
                    sideDatabase = sourceTableInfo.getDatabase();
                    SQLCommand createSideTable = sourceTableInfo.getCreateSideTableStmt();
                    System.out.printf("SMG:\tsql to create unshared side table, %s\n",createSideTable.getRawSQL());
                    WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), createSideTable).onDatabase(sideDatabase).forDDL();
                    sourceTempWG.execute(WorkerGroup.MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
                }

                //TODO: Incremental support will require LIMIT/OFFSET logic, not included here because it will force looping in the handler.
                SQLCommand sourceSQL = sourceTableInfo.getDataChunkQuery();

                // Prepare the source query
                final MysqlPrepareStatementCollector sourcePrepareCollector = new MysqlPrepareStatementCollector();
                WorkerExecuteRequest sourcePreparedQuery = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), sourceSQL).onDatabase(sourceTableInfo.getDatabase());
                sourceWG.execute(WorkerGroup.MappingSolution.AllWorkers, sourcePreparedQuery, sourcePrepareCollector);
                System.out.printf("SMG:\tprepared source query, prepare ID = %s\n", sourcePrepareCollector.getStmtID());

                //This call ensures that the target sites are all using the target database before we try and prepare and insert.
                WorkerExecuteRequest emptyRequest = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), SQLCommand.EMPTY).onDatabase(sourceTableInfo.getDatabase());
                targetWG.execute(WorkerGroup.MappingSolution.AllWorkers, emptyRequest, NoopConsumer.SINGLETON);

                final int[] distKeyOffsets = sourceTableInfo.getDistKeyOffsets();

                final MysqlMessage sourceExecute = MSPComStmtExecuteRequestMessage.newMessage((int)sourcePrepareCollector.getStmtID(),sourcePrepareCollector.getPreparedStatement(),new ArrayList<Object>());

                final KeyValue distKeySample = sourceTableInfo.getEmptyDistKey();
                final AtomicReference<RebalanceRowProcessor> rowProcessorAtomicReference = new AtomicReference<>(null);

                System.out.printf("SMG: insert into side table prefix = %s\n",sourceTableInfo.getSideInsertPrefix());
                GroupDispatch dispatch = new GroupDispatch() {
                    @Override
                    public void setSenderCount(int senderCount) {
                    }

                    @Override
                    public Bundle getDispatchBundle(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
                        RebalanceRowProcessor rowCounter = null;
                        try {
                            rowCounter = new RebalanceRowProcessor(sourcePrepareCollector.getResultColumns(), sourceTableInfo.getSrcInsertPrefix(),distKeySample, distKeyOffsets, deleteChannel, targetWG, sourceTableInfo, promise);
                        } catch (PEException e) {
                            promise.failure(e);
                            return Bundle.NO_COMM;
                        }
                        rowProcessorAtomicReference.set(rowCounter);
                        return new Bundle(sourceExecute, rowCounter);
                    }
                };

                sourceWG.execute(WorkerGroup.MappingSolution.AllWorkers, sourcePreparedQuery, dispatch);
                RebalanceRowProcessor rowProcessor = rowProcessorAtomicReference.get();

                WorkerExecuteRequest closeRequest = new WorkerExecuteRequest(ssCon.getTransactionalContext(), SQLCommand.EMPTY).onDatabase(sourceTableInfo.getDatabase());
                sourceWG.execute(WorkerGroup.MappingSolution.AllWorkers,closeRequest,new MysqlStmtCloseDiscarder(sourcePrepareCollector.getStmtID()));

                //TODO: do we need to commit the side table rows, so that the select transaction can see them?

                System.out.printf("SMG:\tRow placement [source==> site=%s,database=%s,table=%s], total read=%s\n", oldSite.getName(), sourceTableInfo.getDatabase().getName(), sourceTableInfo.getName(), rowProcessor.getRowsRead());
                for (Map.Entry<StorageSite,Long> siteCount : rowProcessor.getFinalRowCounts().entrySet()){
                    System.out.printf("SMG:\t\t%s == %s\n",siteCount.getKey().getName(),siteCount.getValue());
                }

                //OK, we've copied all the rows.
                SQLCommand deleteMovedRows = sourceTableInfo.getSideDelete();

                //SMG:
                String dirtySysbenchHack = deleteMovedRows.getRawSQL().replace("sbtest.", "");
                deleteMovedRows = new SQLCommand(new GenericSQLCommand(CharsetUtil.UTF_8,dirtySysbenchHack));
                System.out.printf("SMG:\thacked sql to delete via table, %s\n",deleteMovedRows.getRawSQL());

                WorkerExecuteRequest deleteJoinRequest = new WorkerExecuteRequest(ssCon.getTransactionalContext(), deleteMovedRows).onDatabase(sourceTableInfo.getDatabase());
                MysqlTextResultCollector textResultCollector = new MysqlTextResultCollector();
                sourceWG.execute(WorkerGroup.MappingSolution.AllWorkers,deleteJoinRequest,textResultCollector);
                System.out.printf("SMG:\texecuted delete join, database was %s , affected row count was %s\n",sourceTableInfo.getDatabase().getName(), textResultCollector.getNumRowsAffected());

                if ( ! rebalanceInfo.hasSharedTable() ) {
                    SQLCommand dropSideTable = sourceTableInfo.getDropSideTableStmt();
                    System.out.printf("SMG:\tsql to drop unshared side table, %s\n",dropSideTable.getRawSQL());
                    //SMG:
//                    WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), dropSideTable).onDatabase(sourceTableInfo.getDatabase()).forDDL();
//                    sourceTempWG.execute(WorkerGroup.MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
                }

            }

            //TODO: now all the rows have been moved+deleted, do we need to deal with innodb table fragmentation?

            if ( rebalanceInfo.hasSharedTable() ) {
                SQLCommand deleteSharedTable = rebalanceInfo.getDropSharedTable();
                System.out.printf("SMG:\tsql to drop shared table, %s\n",deleteSharedTable.getRawSQL());
                //SMG:
//                WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), deleteSharedTable).forDDL();
//                sourceTempWG.execute(WorkerGroup.MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
            }

            //TODO: can we do this here?  Think we need to commit the XA before we can return these to the pool.
            //return the transient source and delete workgroups to the pool.
            WorkerGroup.WorkerGroupFactory.returnInstance(ssCon, sourceWG);
            WorkerGroup.WorkerGroupFactory.returnInstance(ssCon, sourceTempWG);
            WorkerGroup.WorkerGroupFactory.returnInstance(ssCon, targetWG);
        } catch (Exception e){
            //SMG:
            e.printStackTrace();
            throw e;
        }
    }

    public class RebalanceRowProcessor implements MysqlCommandResultsProcessor, RedistTargetSite.InsertWatcher, RedistTargetSite.InsertPolicy, RedistTargetSite.SourceControl {

        ColumnSet columns;
        boolean finished = false;
        long rowsRead = 0L;
        SQLCommand dataInsertPrefix;

        boolean receivedLastRow;

        Map<StorageSite, Long> rowCounts = new LinkedHashMap<>();
        RedistTargetSet targets;
        RedistTargetSite deleteTarget;
        AddStorageGenRangeTableInfo sourceTableInfo;

        KeyValue distKey;
        int[] positionForDistKeyPart;
        CommandChannel deleteChannel;
        CompletionTarget<Boolean> promise;
        ChannelHandlerContext sourceContext;

        public RebalanceRowProcessor(ColumnSet columns, SQLCommand srcInsertPrefix, KeyValue distKey, int[] positionForDistKeyPart, CommandChannel deleteChannel, WorkerGroup targetWG, AddStorageGenRangeTableInfo sourceTableInfo, CompletionTarget<Boolean> promise) throws PEException {
            this.columns = columns;
            this.dataInsertPrefix = srcInsertPrefix;
            this.distKey = distKey;
            this.positionForDistKeyPart = positionForDistKeyPart;
            this.deleteChannel = deleteChannel;
            this.sourceTableInfo = sourceTableInfo;
            this.promise = promise;

            targets = new RedistTargetSet(targetWG,this,this,this);
            deleteTarget = buildDeleteTarget(deleteChannel);

            //set initial row counts for all target sites (and also the source site).
            rowCounts.put(oldSite,0L);
            for (StorageSite targetSite : targetGen.getStorageSites()){
                rowCounts.put(targetSite,0L);
                //make sure we have a target for each listed target.
            }

        }

        private RedistTargetSite buildDeleteTarget(CommandChannel deleteChannel) {
            return new RedistTargetSite(new RedistTargetSite.InsertWatcher() {
                @Override
                public void insertOK(RedistTargetSite site, MyOKResponse okPacket) {
                    System.out.printf("SMG: inserted distkeys into tracking table OK, row count=%s\n",okPacket.getAffectedRows());
                    checkFinished();
                }

                @Override
                public void insertFailed(RedistTargetSite site, MyErrorResponse errorPacket) {
                    RebalanceRowProcessor.this.failure(errorPacket.asException());
                }

                @Override
                public void insertFailed(RedistTargetSite site, Exception e) {
                    RebalanceRowProcessor.this.failure(e);
                }
            },deleteChannel,new RedistTargetSite.InsertPolicy() {
                @Override
                public int getMaximumRowsToBuffer() {
                    return 100;
                }

                @Override
                public long getMaximumBytesToBuffer() {
                    return 16000000;
                }

                @Override
                public int getColumnsPerTuple() {
                    return positionForDistKeyPart.length;
                }

                @Override
                public SQLCommand buildInsertStatement(int tupleCount) throws PEException {
                    StringBuilder valueBubble = new StringBuilder("(?");
                    for (int i=1;i<sourceTableInfo.getDistKeyOffsets().length;i++){
                        valueBubble.append(",?");
                    }
                    valueBubble.append(')');
                    String valueBubbleString = valueBubble.toString();

                    String initialInsertPrefix = sourceTableInfo.getSideInsertPrefix().getRawSQL();
                    //SMG: need to deal with INSERT IGNORE
                    initialInsertPrefix = initialInsertPrefix.replace("INSERT ","INSERT IGNORE ");
                    StringBuilder insertStatement = new StringBuilder(initialInsertPrefix);
                    insertStatement.append(" VALUES ");
                    insertStatement.append(valueBubbleString);
                    for (int i=1;i<tupleCount;i++) {
                        insertStatement.append(',');
                        insertStatement.append(valueBubbleString);
                    }

                    return new SQLCommand(CharsetUtil.UTF_8,insertStatement.toString());
                }

            });
        }


        public long getRowsRead(){
            return rowsRead;
        }


        public Map<StorageSite,Long> getFinalRowCounts(){
            return rowCounts;
        }

        @Override
        public void active(ChannelHandlerContext ctx) {
            this.sourceContext = ctx;
        }

        @Override
        public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
            if (finished)
                return false;

            if (message instanceof MyTextResultRow || message instanceof MyBinaryResultRow) {
                rowsRead++;
                if (message instanceof  MyBinaryResultRow){
                    //prepared select.
                    KeyValue copyDistKey = new KeyValue(distKey);
                    MyBinaryResultRow binRow = (MyBinaryResultRow)message;

                    ArrayList<ColumnDatum> distKeyCols = new ArrayList<>(copyDistKey.values());//get back the entries in insertion order.
                    for (int i=0;i<positionForDistKeyPart.length;i++){
                        int columnInResultRow = positionForDistKeyPart[i];
                        Object value = binRow.getValue(columnInResultRow);
                        ColumnDatum datum = distKeyCols.get(i);
                        datum.setValue(value);
                    }

                    WorkerGroup.MappingSolution mapping = range.getMappingInGeneration(copyDistKey,targetGen);//bypasses normal generation search, since we are moving from older gen to newer gen.
                    StorageSite chosenSite = mapping.getSite();
                    if (oldSite.equals(chosenSite)) {
                        //no movement, row stays on oldSite.
                        incrementRows(chosenSite);
                    } else {
                        //moving row from oldSite to chosenSite.
                        incrementRows(chosenSite);
                        targets.sendInsert(mapping,binRow);

                        MyBinaryResultRow distKeyRow = binRow.projection(positionForDistKeyPart);
                        deleteTarget.append(distKeyRow);

                    }
                }
            } else if (message instanceof MyEOFPktResponse){
                //handled by end()
            } else if (message instanceof MyErrorResponse) {
                this.failure(((MyErrorResponse)message).asException());
            }
            checkFinished();
            return true;
        }

        private void checkFinished(){
            if (finished)
                return;
            else {
                boolean nowFinished = receivedLastRow && !targets.hasPendingRows() && !deleteTarget.hasPendingRows();
                if (nowFinished) {
                    finished = true;
                    promise.success(true);
                    resumeSourceStreams();
                }
            }
        }

        private void incrementRows(StorageSite chosenSite) {
            long existing = rowCounts.get(chosenSite);
            rowCounts.put(chosenSite,existing+1);
        }

        @Override
        public void packetStall(ChannelHandlerContext ctx) throws PEException {
        }

        @Override
        public void failure(Exception e) {
            System.out.printf("SMG: rebalance failed.  \n");
            e.printStackTrace(System.out);
            if (!finished) {
                finished = true;
                promise.failure(e);
                resumeSourceStreams();
            }
        }

        @Override
        public void end(ChannelHandlerContext ctx) {
            System.out.printf("SMG: received last source row.\n");
            receivedLastRow = true;
            targets.flush();
            deleteTarget.flush();
            checkFinished();
        }

        @Override
        public int getMaximumRowsToBuffer() {
            return 100;
        }

        @Override
        public long getMaximumBytesToBuffer() {
            return 16000000L;
        }

        @Override
        public int getColumnsPerTuple() {
            return columns.size();
        }

        @Override
        public SQLCommand buildInsertStatement(int tupleCount) throws PEException {
            int colsPerTuple = columns.size();

            StringBuilder tupleString = new StringBuilder();
            tupleString.append("(");
            if (colsPerTuple > 0)
                tupleString.append("?");
            for (int i=1;i<colsPerTuple;i++){
                tupleString.append(",?");
            }
            tupleString.append(")");
            StringBuilder insertStmt = new StringBuilder();
            insertStmt.append(dataInsertPrefix.getRawSQL());
            if (tupleCount > 0) {
                insertStmt.append(" VALUES ");
                insertStmt.append(tupleString);
                for (int i = 1; i < tupleCount; i++) {
                    insertStmt.append(',');
                    insertStmt.append(tupleString);
                }
            }
//            System.out.printf("SMG:\t insertStmt ==> %s\n",insertStmt.toString());
            return new SQLCommand(new GenericSQLCommand(dataInsertPrefix.getEncoding(),insertStmt.toString()));
        }

        @Override
        public void insertOK(RedistTargetSite site, MyOKResponse okPacket) {
            System.out.printf("SMG: inserted rows into target site OK, row count=%s\n",okPacket.getAffectedRows());
            checkFinished();
        }

        @Override
        public void insertFailed(RedistTargetSite site, MyErrorResponse errorPacket) {
            System.out.printf("SMG: insert failed (error packet) \n");
            failure(errorPacket.asException());
        }

        @Override
        public void insertFailed(RedistTargetSite site, Exception e) {
            System.out.printf("SMG: insert failed (exception) \n");
            failure(e);
        }

        @Override
        public void pauseSourceStreams() {
            System.out.printf("SMG: pause source streams\n");
            if (sourceContext != null) {
                StreamValve.pipelinePause(sourceContext.pipeline());
            }
        }

        @Override
        public void resumeSourceStreams() {
            System.out.printf("SMG: resume source streams\n");
            if (sourceContext != null) {
                StreamValve.pipelineResume(sourceContext.pipeline());
            }
        }
    }
}
