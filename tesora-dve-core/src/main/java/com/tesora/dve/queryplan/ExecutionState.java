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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.db.LateBoundConstants;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.SSContext;

public class ExecutionState {

	private static final LateBoundConstants emptyConstants = new LateBoundConstants();
	
	private final SSConnection connection;
	private final LateBoundConstants constants;
	
	public ExecutionState(SSConnection conn) {
		this(conn,emptyConstants);
	}
	
	protected ExecutionState(SSConnection conn, LateBoundConstants constants) {
		this.connection = conn;
		this.constants = constants;
	}
	
	public SSConnection getConnection() {
		return this.connection;
	}
	
	public boolean hasActiveTransaction() {
		return connection.hasActiveTransaction();
	}
	
	public CatalogDAO getCatalogDAO() {
		return connection.getCatalogDAO();
	}
	
	public SSContext getNonTransactionalContext() {
		return connection.getNonTransactionalContext();
	}
	
	public ExecutionState pushConstants(LateBoundConstants constants) {
		return new ExecutionState(connection,constants);
	}
	
	public LateBoundConstants getBoundConstants() {
		return constants;
	}
}
