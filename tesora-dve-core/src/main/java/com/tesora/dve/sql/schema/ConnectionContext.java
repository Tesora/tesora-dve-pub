// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.infomessage.ConnectionMessageManager;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.variable.VariableAccessor;

public interface ConnectionContext {

	public void setCurrentTenant(IPETenant ten);

	public SchemaEdge<PEUser> getUser();
	public SchemaEdge<IPETenant> getCurrentTenant();	
	public SchemaEdge<Database<?>> getCurrentDatabase();
		
	public void setCurrentDatabase(Database<?> db);
	
	public boolean allowTenantColumnDecls();
	
	public void setSchemaContext(SchemaContext sc);
	
	public String getVariableValue(VariableAccessor va) throws PEException;
	
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
}
