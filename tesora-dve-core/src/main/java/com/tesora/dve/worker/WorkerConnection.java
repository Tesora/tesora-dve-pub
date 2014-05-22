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
