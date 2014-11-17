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

import java.nio.charset.Charset;
import java.util.List;

import com.tesora.dve.db.NoopConsumer;
import com.tesora.dve.db.mysql.RedistTupleBuilder;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.MysqlStmtCloseDiscarder;
import com.tesora.dve.db.mysql.MysqlPrepareStatementCollector;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.worker.AggregationGroup;
import com.tesora.dve.worker.MysqlRedistTupleForwarder;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class QueryStepMultiTupleRedistOperation extends QueryStepDMLOperation {

	static Logger logger = Logger.getLogger( QueryStepMultiTupleRedistOperation.class );
	
	final SQLCommand command;
	SQLCommand insertOptions = null;
	
	final DistributionModel sourceDistModel;
	
	StorageGroup targetGroup = null;
	
	String tempTableName;
	// temp tables are usually distributed static or broadcast, but for certain cases (complex deletes, updates)
	// they have to be distributed the same way as the original table was deleted, and for that we need to know what
	// table they should be distributed like.  in particular, if the table they should be distributed like is range distributed
	// we have to use the range distributed table, because the temp table won't have the right relationships in the catalog.
	PersistentTable distributeTempTableLike;
	
	PersistentTable targetTable;
	TableHints tableHints = TableHints.EMPTY_HINT;
	TempTableDeclHints tempHints;
	
	DistributionModel targetDistModel;
	boolean useResultSetAliases = false;
	
	private List<String> distColumns = null;

	private IKeyValue specifiedDistKeyValue = null;

	private PersistentDatabase targetUserDatabase;

	// for slow query support - record the total number of rows redistributed
	private int totalRows = -1;

	private long executeCounter = 0;
	private UserTable[] createdTempTables = new UserTable[4];
	
	private boolean enforceScalarValue;
	private boolean insertIgnore = false;
	
	private boolean usesUserlandTemporaryTables = false;
	
	private TempTableGenerator tempTableGenerator = TempTableGenerator.DEFAULT_GENERATOR;
	// well, this is a bit of a hack - for the case where the target group is not the same as the source group
	// if there is alread a worker group allocated - use this one, not the one we would get
	private WorkerGroup preallocatedTargetWorkerGroup;
	
	public void setPreallocatedTargetWorkerGroup(WorkerGroup wg) {
		preallocatedTargetWorkerGroup = wg;
	}
	
	public QueryStepMultiTupleRedistOperation(StorageGroup sg, PersistentDatabase execCtxDB, SQLCommand command, DistributionModel sourceDistModel) throws PEException {
		super(sg, execCtxDB);
		this.command = command;
		this.sourceDistModel = sourceDistModel;
		this.targetDistModel = BroadcastDistributionModel.SINGLETON;
	}
	
	
	public QueryStepMultiTupleRedistOperation toTempTable(StorageGroup targetGroup, PersistentDatabase targetDBName, String tempTableName) {
		return toTempTable(targetGroup, targetDBName, tempTableName, false);
	}
	
	public QueryStepMultiTupleRedistOperation toTempTable(StorageGroup targetGroup, PersistentDatabase targetDBName, String tempTableName, boolean resultSetAliases) {
		this.targetGroup = targetGroup;
		this.targetUserDatabase = targetDBName;
		this.tempTableName = tempTableName;
		this.targetTable = null;
		this.useResultSetAliases = resultSetAliases;
		return this;
	}
	
	public QueryStepMultiTupleRedistOperation toUserTable(StorageGroup targetGroup, PersistentTable userTable) {
		return toUserTable(targetGroup, userTable, null, false);
	}	
	
	public QueryStepMultiTupleRedistOperation toUserTable(StorageGroup targetGroup, PersistentTable userTable, TableHints hints, boolean resultSetAliases) {
		this.targetGroup = targetGroup;
		this.targetTable = userTable;
		this.targetUserDatabase = userTable.getDatabase();
		this.tempTableName = null;
		if (hints != null) this.tableHints = hints;
		this.useResultSetAliases = resultSetAliases;
		return this;
	}
	
	public QueryStepMultiTupleRedistOperation distributeOn(List<String> columnNames) {
		this.targetDistModel = StaticDistributionModel.SINGLETON;
		this.distColumns = columnNames;
		return this;
	}

	public QueryStepMultiTupleRedistOperation distributeOn(List<String> columnNames, PersistentTable distributeLike) {
		this.targetDistModel = null;
		this.distColumns = columnNames;
		this.distributeTempTableLike = distributeLike;
		return this;
	}
	
	public QueryStepMultiTupleRedistOperation onDupKey(SQLCommand sqlc) {
		this.insertOptions = sqlc;
		return this;
	}
	
	public QueryStepMultiTupleRedistOperation withTempHints(TempTableDeclHints hints) {
		this.tempHints = hints;
		return this;
	}

	public QueryStepMultiTupleRedistOperation withTableGenerator(TempTableGenerator generator) {
		this.tempTableGenerator = generator;
		return this;
	}
	
	public QueryStepMultiTupleRedistOperation withUserlandTemporaryTables() {
		this.usesUserlandTemporaryTables = true;
		return this;
	}
	
	/**
	 * Called by <b>QueryStep</b> to do the redistribution operation.
	 * <p/>
	 * The <em>command</em> is executed in the source database against the provided {@link WorkerGroup}.
	 * The metadata from the results is used to create the temporary table on the target {@link PersistentGroup},
	 * then the results are mapped through the appropriate {@link DistributionModel} and sent to
	 * the temporary table on the appropriate {@link PersistentSite}.
	 * 
	 * @returns number of rows transferred
	 */
	@Override
	public void executeSelf(ExecutionState estate, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
		
		if (targetGroup == null)
			throw new PEException("QueryStepRedistOperation not properly initialized (must call toTempTable or toUserTable)");
		
		final SSConnection ssCon = estate.getConnection();
		
		// TODO: we should have a RedistributeRequest which makes the
		// workers do the redistribution, saving a hop of the data
		
		// If we're redistributing to the same group that we're reading from, use the source
		// group as the target so that the records appear within the transactional context of 
		// the source group.  But, we need to execute the create temp table DDL in a separate transactional
		// context either way.
		//

		WorkerGroup sourceWG = null; 
		WorkerGroup targetWG = null; 
		WorkerGroup allocatedWG = null;
		WorkerGroup cleanupWG = null;

		PersistentTable targetTable = this.targetTable;

		beginExecution();
		// determine the source and dest worker groups
		if (!targetGroup.equals(wg.getGroup())) {
			sourceWG = wg;
			if (preallocatedTargetWorkerGroup != null && preallocatedTargetWorkerGroup.getGroup().equals(targetGroup))
				allocatedWG = preallocatedTargetWorkerGroup;
			else
				allocatedWG = ssCon.getWorkerGroup(targetGroup,getContextDatabase());
			if (logger.isDebugEnabled())
				logger.debug("Redist allocates for different storage group: " + allocatedWG);
			targetWG = allocatedWG;

			doRedistribution(estate, resultConsumer, /* useSystemTempTable */ targetGroup.isTemporaryGroup(), tempTableName,
					sourceWG, database, sourceDistModel, bindCommand(estate,command),
					specifiedDistKeyValue, distColumns, distributeTempTableLike,
					targetWG, targetUserDatabase, targetDistModel, targetTable, 
					tableHints, tempHints, insertOptions, allocatedWG, /* cleanupWG */ null,
					tempTableGenerator,0);
		} else if ((ssCon.hasActiveTransaction() && wg.isModified() && targetTable != null) || usesUserlandTemporaryTables) {
			// Here we want to redistribute from a persistent group back into itself, within the context
			// of a transaction.  However, we must both read within the context of the transaction, and
			// write within the context of a transaction, but we can't both read and write on the same
			// set of connections, and transactions exist only within the scope of the connection.
			// To get around this, we must redistribute the data out to a dynamic site, then
			// redistribute it back in.
			
			CatalogDAO c = ssCon.getCatalogDAO();
			StorageGroup cacheGroup = new AggregationGroup(
					c.findDynamicPolicy(KnownVariables.DYNAMIC_POLICY.getSessionValue(ssCon)));
			WorkerGroup cacheWG = WorkerGroupFactory.newInstance(ssCon, cacheGroup, getContextDatabase());
			
			try {
				String intermediateTempTableName = UserTable.getNewTempTableName();
				PersistentTable tempTable = 
				doRedistribution(estate, resultConsumer, /* useSystemTempTable */ true, intermediateTempTableName,
						/* sourceWG */ wg, database, sourceDistModel, bindCommand(estate,command),
						specifiedDistKeyValue, distColumns, /* distributeTempTableLike */ null,
						/* targetWG */ cacheWG, targetUserDatabase, BroadcastDistributionModel.SINGLETON, /* targetTable */ null, 
						tableHints, /* tempHints */ null, /* insertOptions */ null, /* allocatedWG */ null, /* cleanupWG */ null,
						TempTableGenerator.DEFAULT_GENERATOR,1);
				
				SQLCommand tempQuery = new SQLCommand(ssCon, "select * from " + tempTable.getNameAsIdentifier());
				doRedistribution(estate, resultConsumer, /* useSystemTempTable */ false, tempTableName,
						cacheWG, targetUserDatabase, BroadcastDistributionModel.SINGLETON, tempQuery,
						/* specifiedDistKeyValue */ null, /* distColumns */ null, distributeTempTableLike,
						/* targetWG */ wg, targetUserDatabase, targetDistModel, targetTable, 
						/* tableHints */ TableHints.EMPTY_HINT, tempHints, insertOptions, /* allocatedWG */ null, /* cleanupWG */ null,
						tempTableGenerator,2);
			} catch (Exception e) {
				cacheWG.markForPurge();
				throw e;
			} finally {
				WorkerGroupFactory.returnInstance(ssCon, cacheWG);
			}
			
			// We might need to load all the source data into memory or a dynamic site here, but for
			// now we will try using Netty to buffer the outgoing records
//			throw new PECodingException("Redist back to user table within context of a transaction with previous changes is not supported");
			//				sourceWG = wg;
			//				targetWG = wg;
		} else {
			if (wg.getGroup().isTemporaryGroup() || targetTable == null) {
				// We must read from the provided wg in case it is holding a TEMPORARY table that only 
				// exists on that connection. We don't like doing this step as we must clone the workerGroup,
				// rather than getting one from the cache.
				sourceWG = wg;
				allocatedWG = wg.clone(ssCon);
				if (logger.isDebugEnabled())
					logger.debug("Redist allocates clone for temp group/table: " + allocatedWG);
				targetWG = allocatedWG;
				cleanupWG = sourceWG;
				if (logger.isDebugEnabled())
					logger.debug("Doing same group redist to temporary group(" + sourceWG + " => " + targetTable + ")");
				//					ssCon.swapWorkerGroup(wg, allocatedWG);
			} else {
				// We are redisting from the persistent group back to itself, but we know that it hasn't
				// been modified within the scope of the current transaction so we can read from a 
				// second connection
				allocatedWG = WorkerGroupFactory.newInstance(ssCon, wg.getGroup(), getContextDatabase());
				if (logger.isDebugEnabled())
					logger.debug("Redist allocates for redist back to persistent group: " + allocatedWG);
				sourceWG = allocatedWG;
				targetWG = wg;
				if (logger.isDebugEnabled())
					logger.debug("Doing same group redist to persistent group(" + sourceWG + " => " + targetTable + ")");
				allocatedWG.markForPurge();
			}

			doRedistribution(estate, resultConsumer, /* useSystemTempTable */ false, tempTableName,
					sourceWG, database, sourceDistModel, bindCommand(estate,command),
					specifiedDistKeyValue, distColumns, distributeTempTableLike,
					targetWG, targetUserDatabase, targetDistModel, targetTable, 
					tableHints, tempHints, insertOptions, allocatedWG, cleanupWG,
					tempTableGenerator,3);
		}

		endExecution(totalRows);
		executeCounter++;
	}


	private PersistentTable doRedistribution(ExecutionState estate, 
			DBResultConsumer resultConsumer, 
			boolean useSystemTempTable, 
			String givenTempTableName,
			WorkerGroup sourceWG, 
			PersistentDatabase sourceDatabase, 
			DistributionModel givenSourceDistModel, 
			SQLCommand givenCommand,
			IKeyValue givenSpecifiedDistKeyValue, 
			List<String> givenDistColumns, 
			PersistentTable givenDistributeTempTableLike,
			WorkerGroup targetWG, 
			PersistentDatabase givenTargetUserDatabase, 
			DistributionModel givenTargetDistModel, 
			PersistentTable givenTargetTable, 
			TableHints givenTableHints, 
			TempTableDeclHints givenTempHints, 
			SQLCommand givenInsertOptions, 
			WorkerGroup allocatedWG,
			WorkerGroup cleanupWG,
			TempTableGenerator tableGenerator,
			int branch) throws PEException {
		
		final SSConnection ssCon = estate.getConnection();
		
		CatalogDAO c = ssCon.getCatalogDAO();
		
		long rowcount = 0;
		
		try {

			if (sourceWG == targetWG)
				throw new PECodingException("Cannot redist WorkerGroup " + sourceWG + " back to itself");

			if (logger.isDebugEnabled())
				logger.debug(ssCon + ": Redisting from " + sourceWG + " to " + targetWG);

			//			System.out.println("Redist from " + sourceWG + " to " + targetWG);

			// Prepare the source query
			MysqlPrepareStatementCollector selectCollector = new MysqlPrepareStatementCollector();
			MappingSolution sourceWorkerMapping = null;
			if (givenSpecifiedDistKeyValue != null) {
				sourceWorkerMapping = QueryStepOperation.mapKeyForQuery(estate, sourceWG.getGroup(), givenSpecifiedDistKeyValue, givenSourceDistModel, DistKeyOpType.QUERY);
			} else {
				sourceWorkerMapping = givenSourceDistModel.mapForQuery(sourceWG, givenCommand);
			}
			//			MappingSolution sourceWorkerMapping = sourceDistModel.mapForQuery(wg, command);
			WorkerExecuteRequest redistQueryRequest = 
					new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), givenCommand).onDatabase(sourceDatabase);
			if (logger.isDebugEnabled())
				logger.debug(ssCon + ": Redist: preparing source query " + redistQueryRequest);
			sourceWG.execute(sourceWorkerMapping, redistQueryRequest, selectCollector);

			// Create the temp table
			final ColumnSet resultMetadata = givenTableHints.addAutoIncMetadata(selectCollector.getResultColumns());

			if (givenTargetTable == null) {
				if (cleanupWG == null)
					cleanupWG = targetWG;
				UserTable existing = null;
				if (executeCounter > 0)
					existing = createdTempTables[branch];
				givenTargetTable = tableGenerator.createTable(ssCon, targetWG, cleanupWG,
						givenTempHints, useSystemTempTable, givenTempTableName,
						givenTargetUserDatabase, resultMetadata, givenTargetDistModel,existing);
				if (executeCounter == 0)
					createdTempTables[branch] = (UserTable) givenTargetTable;
				if (logger.isDebugEnabled())
					logger.debug(ssCon + ": Redist: Created temp table " + givenTargetTable);
				//				WorkerExecuteRequest lockReq = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), new SQLCommand("LOCK TABLES " + targetTable.getNameAsIdentifier() + " WRITE"));
				//				targetWG.execute(MappingSolution.AllWorkers, lockReq, DBEmptyTextResultConsumer.INSTANCE);
			}

			// Use a thread to build the default insert statement (assuming we'll need it more often than not)
			final PersistentTable tableForInsertStatement = givenTargetTable;
			final int tableColumnCount = tableForInsertStatement.getNumberOfColumns(); 
			final int maxColumnCount = KnownVariables.REDIST_MAX_COLUMNS.getGlobalValue(ssCon).intValue(); 
			final int maxTupleCount = (maxColumnCount > tableColumnCount) ? (maxColumnCount / tableColumnCount) : 1;
			final int maxDataSize = KnownVariables.REDIST_MAX_SIZE.getGlobalValue(ssCon).intValue(); 
			//			Future<SQLCommand> insertStatementFuture = Host.submit(new Callable<SQLCommand>() {
			//				public SQLCommand call() throws Exception {
			//					return getTableInsertStatement(tableForInsertStatement, insertOptions, resultMetadata, maxTupleCount);
			//				}
			//			});

            //			// Prepare the insert statement on the target worker group
			//			SQLCommand insertStatement = getTableInsertStatement(targetTable, resultMetadata);
			//			WorkerExecuteRequest redistInsertRequest = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), insertStatement).onDatabase(targetUserDatabase);
			//			MysqlPrepareStatementCollector insertCollector = new MysqlPrepareStatementCollector();
			//			if (logger.isDebugEnabled())
			//				logger.debug(ssCon + ": Redist: preparing instert statement on target group: " + redistInsertRequest);
			//			targetWG.execute(MappingSolution.AllWorkers, redistInsertRequest, insertCollector);
			////			System.out.println("insertCollector A " + insertCollector.getPreparedStatement());


            // Start the redistribution
            PersistentTable distributeTableLike = (givenDistributeTempTableLike == null ? givenTargetTable : givenDistributeTempTableLike);
            KeyValue dv = null;
            if (givenDistColumns == null)
                dv = distributeTableLike.getDistValue(ssCon.getCatalogDAO());
            else
                dv = new KeyValue(distributeTableLike,distributeTableLike.getRangeID(c),givenDistColumns);

            RedistTupleBuilder newBuilder = new RedistTupleBuilder(c, distributeTableLike.getDistributionModel(), givenInsertOptions, givenTargetTable, maxTupleCount, maxDataSize, targetWG);
            newBuilder.setInsertIgnore(insertIgnore);

            //TODO: It would be nicer if we didn't have to set database on all the target sites up front.
			WorkerExecuteRequest emptyRequest = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), SQLCommand.EMPTY).onDatabase(givenTargetUserDatabase);
            targetWG.execute(MappingSolution.AllWorkers, emptyRequest, NoopConsumer.SINGLETON);

			MysqlRedistTupleForwarder redistForwarder = 
					new MysqlRedistTupleForwarder(
							dv, givenTableHints,
							useResultSetAliases, selectCollector.getPreparedStatement(), newBuilder);
			if (logger.isDebugEnabled())
				logger.debug(ssCon + ": Redist: starting redistribution: " + redistForwarder);
			sourceWG.execute(sourceWorkerMapping, redistQueryRequest, redistForwarder);
			if (logger.isDebugEnabled()) logger.debug("Redist sender completes");

			// Everything is sent now, so sync up with the results handler
			@SuppressWarnings("unused")
			int recordsSent = redistForwarder.getNumRowsForwarded();
			int recordsInserted = newBuilder.getUpdateCount();
			rowcount = recordsInserted;
			
			// Close the prepared statements
			//			System.out.println("selectCollector " + selectCollector.getPreparedStatement());
			sourceWG.execute(sourceWorkerMapping, new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), SQLCommand.EMPTY).onDatabase(sourceDatabase), 
					new MysqlStmtCloseDiscarder(selectCollector.getPreparedStatement()));
			//			System.out.println("insertCollector B " + insertCollector.getPreparedStatement());
			//			targetWG.submit(MappingSolution.AllWorkers, emptyRequest, 
			//					new MysqlStmtCloseDiscarder(insertCollector.getPreparedStatement()));

			//			if (targetTable.isTempTable()) {
			//				WorkerExecuteRequest lockReq = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), new SQLCommand("UNLOCK TABLES"));
			//				targetWG.execute(MappingSolution.AllWorkers, lockReq, DBEmptyTextResultConsumer.INSTANCE);
			//			}

			if (!targetWG.getGroup().isTemporaryGroup() && givenTargetTable.getDistributionModel() != null) {
				long adjustedRowCount = givenTargetTable.getDistributionModel().getInsertAdjuster().adjust(recordsInserted, targetWG.size());
				resultConsumer.setNumRowsAffected(adjustedRowCount);
				rowcount = adjustedRowCount;
			}

			if (enforceScalarValue && resultConsumer.getUpdateCount() > 1) {
				throw new PEException(command + ": Is expected to be a scalar query.");
			}

			if (logger.isDebugEnabled())
				logger.debug(ssCon + ": Redist complete: " + /* recordsSent + " records sent, " + */ recordsInserted + " rows inserted");

			// TOPERF - we can no longer check the row counts as we don't know how many we are sending
			// (in some cases, we send the whole packets without looking at them)
			//			// skip this check if stmt has ON DUPLICATE clause
			//			if ((insertOptions == null) && (recordsSent != recordsInserted))
			//				// might not if there is an on dup key clause - then the per row update count 
			//				// is 2 if the row was updated and 1 if inserted - so we will ignore in that case
			//				throw new PEException("Redistribution count does not match: sent " + recordsSent +
			//						", inserted " + recordsInserted + ", query: " + redistQueryRequest);
		} catch (Exception e) {
			if (allocatedWG != null)
				allocatedWG.markForPurge();
			throw new PEException("Executing redist command: " + command, e);
		} finally {
			if (allocatedWG != null) {
				if (logger.isDebugEnabled())
					logger.debug("Redist deallocates: purge=" + allocatedWG.isMarkedForPurge() + ", wg="+ allocatedWG);
				ssCon.returnWorkerGroup(allocatedWG);
			}
		}
		
		totalRows += rowcount;
		
		return givenTargetTable;
	}

	static public SQLCommand getTableInsertStatement(final Charset connectionCharset, PersistentTable targetTable, SQLCommand insertOptions,
			ColumnSet targetTableColumns, int tupleCount, boolean ignore) throws NumberFormatException, PEException {
		StringBuffer query = new StringBuffer("insert ");
				if (ignore) query.append(" ignore ");
        query.append("into ")
				.append(Singletons.require(HostService.class).getDBNative().getNameForQuery(targetTable))
				.append('(')
				.append(Singletons.require(HostService.class).getDBNative().getNameForQuery(targetTableColumns.getColumn(1)));
		StringBuffer tuplesString = new StringBuffer("(?");
		for (int i = 1; i < targetTableColumns.size(); ++i) {
            query.append(", ").append(Singletons.require(HostService.class).getDBNative().getNameForQuery(targetTableColumns.getColumn(i + 1)));
			tuplesString.append(",?");
		}
		tuplesString.append(')');
		query.append(") values ").append(tuplesString);
		for (int i = 1; i < tupleCount;  ++i)
			query.append(',').append(tuplesString);
		if (insertOptions != null) {
			query.append(" ").append(insertOptions.getRawSQL());
		}
		SQLCommand out = new SQLCommand(connectionCharset, query.toString());
		return out;
	}

//	
//	public SQLCommand getTableInsertStatement(int tupleCount) throws NumberFormatException, PEException {
//		StringBuffer query = new StringBuffer("insert into ")
//				.append(Host.getDBNative().getNameForQuery(this))
//				.append('(')
//				.append(Host.getDBNative().getNameForQuery(userColumns.get(0)));
//		StringBuffer tuplesString = new StringBuffer("(?");
//		for (int i = 1; i < userColumns.size(); ++i) {
//			query.append(", ").append(Host.getDBNative().getNameForQuery(userColumns.get(i)));
//			tuplesString.append(",?");
//		}
//		tuplesString.append(')');
//		query.append(") values ").append(tuplesString);
//		for (int i = 1; i < tupleCount;  ++i)
//			query.append(',').append(tuplesString);
//		SQLCommand out = new SQLCommand(query.toString());
//		out.setWidth(userColumns.size());
//		return out;
//	}

	
	@Override
	public boolean requiresTransactionSelf() {
		return false;
	}
	
	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		buf.append("StreamingRedist ");
		if (tempTableName != null)
			buf.append("temp table=").append(tempTableName);
		else
			buf.append("user table=").append(targetTable.displayName());
		if (totalRows > 0)
			buf.append(", rows=").append(totalRows);
		buf.append(", stmt=").append(command.getRawSQL());
		return buf.toString();
	}

	public IKeyValue getSpecifiedDistKeyValue() {
		return specifiedDistKeyValue;
	}


	public void setSpecifiedDistKeyValue(IKeyValue specifiedDistKeyValue) {
		this.specifiedDistKeyValue = specifiedDistKeyValue;
	}

	public void setEnforceScalarValue(boolean mustEnforceScalarValue) {
		this.enforceScalarValue = mustEnforceScalarValue;
	}
	
	public void setInsertIgnore(boolean ignore) {
		this.insertIgnore = ignore;
	}
}
