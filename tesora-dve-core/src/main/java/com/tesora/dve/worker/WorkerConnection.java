// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.exceptions.PESQLException;

public interface WorkerConnection {
	
	WorkerStatement getStatement(Worker w) throws PESQLException;
	
	void closeActiveStatements() throws PESQLException;
	
	void close(boolean isStateValid) throws PESQLException;

	void setCatalog(String currentDatabaseName) throws PESQLException;

	void rollbackXA(DevXid xid) throws PESQLException;

	void commitXA(DevXid xid, boolean onePhase) throws PESQLException;

	void prepareXA(DevXid xid) throws PESQLException;

	void endXA(DevXid xid) throws PESQLException;

	void startXA(DevXid xid) throws PESQLException;

	boolean isModified() throws PESQLException;

	boolean hasActiveTransaction() throws PESQLException;

	int getConnectionId() throws PESQLException;
}
