// OS_STATUS: public
package com.tesora.dve.worker;

import java.sql.ResultSet;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.messaging.SQLCommand;


public interface WorkerStatement {
	
	boolean execute(int connectionId, SQLCommand sql, DBResultConsumer resultConsumer) throws PESQLException;
	
	void cancel() throws PESQLException;

	void close() throws PESQLException;

	ResultSet getResultSet() throws PESQLException;

	void addBatch(SQLCommand sql) throws PESQLException;
	void clearBatch() throws PESQLException;
	int[] executeBatch() throws PESQLException;	
}
