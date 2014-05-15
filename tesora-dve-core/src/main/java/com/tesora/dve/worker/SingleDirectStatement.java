// OS_STATUS: public
package com.tesora.dve.worker;

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.messaging.SQLCommand;

public class SingleDirectStatement implements WorkerStatement {

	static Logger logger = Logger.getLogger( SingleDirectStatement.class );

	DBConnection dbConnection;

	final Worker worker;

	protected Worker getWorker() {
		return worker;
	}

	public SingleDirectStatement(Worker w, DBConnection dbConnection) throws PESQLException {
		this.worker = w;
		this.dbConnection = dbConnection;
	}
	
	protected boolean doExecute(SQLCommand sqlCommand, DBResultConsumer resultConsumer) throws PESQLException {
		try {
		if (sqlCommand.hasReferenceTime()) {
			String setTimestampSQL = "SET TIMESTAMP=" + sqlCommand.getReferenceTime() + ";";
			dbConnection.execute(new SQLCommand(setTimestampSQL), DBEmptyTextResultConsumer.INSTANCE);
		}

		return dbConnection.execute(sqlCommand.getResolvedCommand(worker), resultConsumer, 
				new PEDefaultPromise<Boolean>()
				).sync();
		} catch (Exception e) {
			if (e instanceof PESQLException)
				throw (PESQLException)e;
			else
				throw new PESQLException(e);
		}
	}
	
	@Override
	public boolean execute(int connectionId, final SQLCommand sql, final DBResultConsumer resultConsumer) throws PESQLException {
		boolean hasResults = false;
		StatementManager.INSTANCE.registerStatement(connectionId, this);
		PerHostConnectionManager.INSTANCE.changeConnectionState(
				connectionId, "Query", "", (sql == null) ? "Null Query" : sql.getRawSQL());
		try {
			
			hasResults = connectionSafeJDBCCall(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return doExecute(sql, resultConsumer);
				}
			});
			
		} catch (PESQLException e) {
			throw new PESQLException(e.getMessage(), new PESQLException("On statement: " + sql.getDisplayForLog(), e));
		} finally {
			StatementManager.INSTANCE.unregisterStatement(connectionId, this);
			PerHostConnectionManager.INSTANCE.resetConnectionState(connectionId);
		}
		return hasResults;
	}
	
	private <T> T connectionSafeJDBCCall(Callable<T> safeCall) throws PESQLException {
		try {
			return safeCall.call();
		} catch (PECommunicationsException c) {
			worker.setLastException(c);
			onCommunicationFailure(worker);
			throw c;
		} catch (PESQLException e) {
			worker.setLastException(e);
			throw e;
		} catch (Exception e) {
			worker.setLastException(e);
			throw new PESQLException(e);
		}
	}

	protected void onCommunicationFailure(Worker worker) throws PESQLException {
	}
	
	@Override
	public void cancel() throws PESQLException {
		if (dbConnection != null) {
			connectionSafeJDBCCall(new Callable<Object>() {

				@Override
				public Boolean call() throws Exception {
					dbConnection.cancel();
					return null;
				}
			});
		}
	}

	@Override
	public void close() throws PESQLException {
		if (dbConnection != null) {
//			connectionSafeJDBCCall(new Callable<Object>() {
//
//				@Override
//				public Boolean call() throws Exception {
//					dbConnection.close();
//					return null;
//				}
//			});
			dbConnection = null;
		}
	}

	@Override
	public ResultSet getResultSet() throws PESQLException {
		return connectionSafeJDBCCall(new Callable<ResultSet>() {

			@Override
			public ResultSet call() throws Exception {
				return null;
//				return dbConnection.getResultSet();
			}
		});
	}

	@Override
	public void addBatch(final SQLCommand sqlCommand) throws PESQLException {
		try {
			connectionSafeJDBCCall(new Callable<Object>() {

				@Override
				public Boolean call() throws Exception {
//					dbConnection.addBatch(sqlCommand.getSQL(worker));
					return null;
				}
			});
		} catch (PESQLException e) {
			throw new PESQLException("Executing: " + sqlCommand, e);
		}
	}

	@Override
	public void clearBatch() throws PESQLException {
		connectionSafeJDBCCall(new Callable<Object>() {

			@Override
			public Boolean call() throws Exception {
//				dbConnection.clearBatch();
				return null;
			}
		});
	}

	@Override
	public int[] executeBatch() throws PESQLException {
		return connectionSafeJDBCCall(new Callable<int[]>() {

			@Override
			public int[] call() throws Exception {
				return null;
//				return dbConnection.executeBatch();
			}
		});
	}
}
