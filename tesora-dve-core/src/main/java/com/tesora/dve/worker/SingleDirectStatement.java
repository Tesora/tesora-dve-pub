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

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.DelegatingCompletionHandle;
import com.tesora.dve.db.DBConnection;
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

	protected void doExecute(SQLCommand sqlCommand, DBResultConsumer resultConsumer, final CompletionHandle<Boolean> promise) {
		if (sqlCommand.hasReferenceTime()) {
            dbConnection.setTimestamp( sqlCommand.getReferenceTime(), null);
		}

        CompletionHandle<Boolean> transformErrors = new DelegatingCompletionHandle<Boolean>(promise) {

            @Override
            public void failure(Exception e) {
                if (e instanceof PESQLException)
                    super.failure(e);
                else {
                    PESQLException convert = new PESQLException(e);
                    convert.fillInStackTrace();
                    super.failure(convert);
                }
            }

        };

		resultConsumer.dispatch(dbConnection, sqlCommand.getResolvedCommand(worker), transformErrors);
	}

    @Override
	public void execute(final int connectionId, final SQLCommand sql, final DBResultConsumer resultConsumer, CompletionHandle<Boolean> promise) {
    	
		StatementManager.INSTANCE.registerStatement(connectionId, this);
		PerHostConnectionManager.INSTANCE.changeConnectionState(
				connectionId, "Query", "", (sql == null) ? "Null Query" : sql.getRawSQL());

        CompletionHandle<Boolean> wrapped = new DelegatingCompletionHandle<Boolean>(promise){
            @Override
            public void success(Boolean returnValue) {
                onFinish();
                super.success(returnValue);
            }

            @Override
            public void failure(Exception e) {
                Exception convert = processException(sql,e);
                onFinish();
                super.failure(convert);
            }

            void onFinish(){
                StatementManager.INSTANCE.unregisterStatement(connectionId, SingleDirectStatement.this);
                PerHostConnectionManager.INSTANCE.resetConnectionState(connectionId);
            }
        };

		doExecute(sql, resultConsumer, wrapped);
	}

    private PESQLException processException(SQLCommand sql, Exception e) {
        PESQLException psqlError;
        if (e instanceof PECommunicationsException){
            try{
                onCommunicationFailure(worker);
            } catch (PESQLException closeError){
                //we got a comm error, then had a failure trying to cleanup, probably because of the comm error.  ignore.
            }
        }

        if (e instanceof PESQLException)
            psqlError = (PESQLException)e;
        else
            psqlError = new PESQLException(e);

        worker.setLastException(psqlError);
        
        if (sql != null)
            return new PESQLException(psqlError.getMessage(), new PESQLException("On statement: " + sql.getDisplayForLog(), psqlError));
        else
            return psqlError;
    }

    private <T> T connectionSafeJDBCCall(Callable<T> safeCall) throws PESQLException {
		try {
			return safeCall.call();
		} catch (Exception e) {
			throw processException(null,e);
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
