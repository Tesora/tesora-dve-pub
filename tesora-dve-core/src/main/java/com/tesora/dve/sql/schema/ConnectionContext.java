package com.tesora.dve.sql.schema;

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

import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.infomessage.ConnectionMessageManager;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.variables.AbstractVariableAccessor;
import com.tesora.dve.variables.VariableStoreSource;

public interface ConnectionContext {

	public void setCurrentTenant(IPETenant ten);

	public SchemaEdge<PEUser> getUser();
	public SchemaEdge<IPETenant> getCurrentTenant();	
	public SchemaEdge<Database<?>> getCurrentDatabase();
		
	public void setCurrentDatabase(Database<?> db);
	
	public boolean allowTenantColumnDecls();
	
	public void setSchemaContext(SchemaContext sc);
	
	public String getVariableValue(AbstractVariableAccessor va) throws PEException;
	
	public List<List<String>> getVariables(VariableScope vs) throws PEException;	
	
	public String getName();
	
	public int getConnectionId();
	
	public void acquireLock(LockSpecification ls, LockType type);
	
	public boolean isInTxn();
	
	public boolean isInXATxn();
	
	public ConnectionContext copy();
	
	public boolean hasFilter();
	public boolean isFilteredTable(Name table);
	public boolean originatedFromReplicationSlave();

	public String getCacheName(); 	
	
	public ConnectionMessageManager getMessageManager();

	public long getLastInsertedId();
	
	public CatalogDAO getDAO();
	
	public VariableStoreSource getVariableSource();
	
}
