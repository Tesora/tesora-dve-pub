package com.tesora.dve.sql.statement.ddl;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.GrantScope;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPriviledge;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.util.Functional;

public class GrantStatement extends DDLStatement {

	private PEPriviledge privilege;
	
	public GrantStatement(PEPriviledge privilege) {
		super(false);
		this.privilege = privilege;
	}

	public GrantScope getGrantScope() {
		return privilege.getGrantScope();
	}
	
	public String getPrivileges() {
		return "ALL";
	}

	public PEUser getUser() {
		return privilege.getUser();
	}
	
	@Override
	public Action getAction() {
		return Action.ALTER;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		return privilege;
	}

	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		pc.beginSaveContext();
		try {
			privilege.persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}
	
	@Override
	public PEDatabase getDatabase(SchemaContext pc) {
		// the database for a grant statement is the database the grant is on - not whatever is current
		if (privilege.getDatabase() != null)
			return privilege.getDatabase();
		else if (privilege.getTenant() != null)
			return privilege.getTenant().getDatabase(pc);
		// if it's a global grant, return null
		return null;
	}
	
	@Override	
	public PEStorageGroup getStorageGroup(SchemaContext pc) {
		// use the default persistent group of the database, or if no database, use the all sites group
		PEDatabase pedb = getDatabase(pc);
		if (pedb != null)
			return pedb.getDefaultStorage(pc);
		else
			return buildAllSitesGroup(pc);
	}
		
	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		String sql = "";
		if (privilege.isGlobal() || true) {
			StringBuilder buf = new StringBuilder();
            Singletons.require(DBNative.class).getEmitter().emitGrantStatement(this, buf);
			sql = buf.toString();
		}
		return new SimpleDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), getRoot(), getAction(), new SQLCommand(pc, sql),
				getDeleteObjects(pc), getCatalogObjects(pc), getInvalidationRecord(pc));
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		// only additive, so nothing to invalidate, I don't think
		return null;
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.GRANT;
	}

}
