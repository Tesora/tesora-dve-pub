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
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.NonMTCachedPlan;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.variable.VariableAccessor;

public class SSConnectionContext implements ConnectionContext {

	protected SSConnection backing;
	protected SchemaContext schemaContext;
	
	public SSConnectionContext(SSConnection conn) {
		backing = conn;
	}

	@Override
	public void setSchemaContext(SchemaContext sc) {
		schemaContext = sc;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public SchemaEdge<IPETenant> getCurrentTenant() {
		return StructuralUtils.buildEdge(schemaContext, backing.getCurrentTenant(), true);
	}

	@Override
	public boolean allowTenantColumnDecls() {
		return false;
	}

	@Override
	public void setCurrentTenant(IPETenant ten) {
		backing.setCurrentTenant(ten);
	}

	@Override
	public String getVariableValue(VariableAccessor va) throws PEException {
		return va.getValue(backing);
	}

	@Override
	public List<List<String>> getVariables(VariableScope vs) throws PEException {
		return VariableAccessor.getValues(vs.getScopeKind(), vs.getScopeName(), backing);
	}

	@Override
	public SchemaEdge<PEUser> getUser() {
		return backing.getUser();
	}

	@Override
	public SchemaEdge<Database<?>> getCurrentDatabase() {
		return backing.getCurrentDatabaseEdge();
	}

	@Override
	public String getName() {
		return backing.getName();
	}

	@Override
	public void acquireLock(LockSpecification ls, LockType type) {
		backing.acquireLock(ls, type);
	}

	@Override
	public boolean isInTxn() {
		return backing.hasActiveTransaction();
	}

	@Override
	public boolean isInXATxn() {
		return backing.hasActiveXATransaction();
	}
	
	@Override
	public ConnectionContext copy() {
		return new SSConnectionContext(backing);
	}

	@Override
	public boolean hasFilter() {
		if (backing != null) {
			return backing.getReplicationOptions().getFilteredTables().size()>0;
		}
		return false;
	}
	
	@Override
	public boolean isFilteredTable(Name table) {
		if (backing != null) {
			Name t = table.getCapitalized();
			for(TableCacheKey k : backing.getReplicationOptions().getFilteredTables()) {
				Name n = new QualifiedName(new UnqualifiedName(k.getDatabaseName()), k.getTableName().getUnqualified());
				if (n.getCapitalized().equals(t)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean originatedFromReplicationSlave() {
		if (backing != null) {
			return backing.getReplicationOptions().connectionFromReplicationSlave();
		}
		return false;
	}
	
	@Override
	public String getCacheName() {
		if (backing != null) {
			return backing.getCacheName();
		}
		return NonMTCachedPlan.GLOBAL_CACHE_NAME;
	}

	@Override
	public int getConnectionId() {
		return backing.getConnectionId();
	}

	@Override
	public ConnectionMessageManager getMessageManager() {
		return backing.getMessageManager();
	}

	@Override
	public long getLastInsertedId() {
		return backing.getLastInsertedId();
	}

	@Override
	public void setCurrentDatabase(Database<?> db) {
		backing.setCurrentDatabase(db);
	}

	@Override
	public CatalogDAO getDAO() {
		return backing.getCatalogDAO();
	}
}
