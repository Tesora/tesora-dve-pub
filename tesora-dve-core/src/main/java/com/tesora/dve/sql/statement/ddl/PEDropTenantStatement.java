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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.worker.WorkerGroup;

public class PEDropTenantStatement extends
		PEDropStatement<PETenant, Tenant> {

	private final StatementType countAs;
	
	public PEDropTenantStatement(Boolean ifExists, Persistable<PETenant, Tenant> targ, StatementType origStatementType) {
		super(PETenant.class, ifExists, true, targ, "TENANT");
		countAs = origStatementType;
	}

	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		if (getTarget() != null) {
			// delete the children before the parent.  the children is all the scoping records.
			Tenant t = getTarget().getPersistent(pc);
			return Collections.singletonList((CatalogEntity)t);
		}
		else
			return Collections.emptyList();
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		// for the target tenant, delete from all the tables that are visible
		PETenant tenant = (PETenant) getTarget();
		ArrayList<TableScope> scopes = new ArrayList<TableScope>(tenant.getTableScopes(pc));
		for(TableScope ts : scopes) {
			DeleteStatement ds = AdaptiveMultitenantSchemaPolicyContext.buildTenantDeleteFromTableStatement(pc, ts.getTable(pc), ts);
			ds.plan(pc,es, config);
		}
		es.append(new ComplexDDLExecutionStep(tenant.getDatabase(pc),tenant.getDatabase(pc).getDefaultStorage(pc),tenant, Action.DROP, 
				new DropTenantCallback(pc,tenant)));
	}
	
	@Override
	public StatementType getStatementType() {
		if (countAs == null) return StatementType.UNIMPORTANT;
		return countAs;
	}
		

	
	private static class DropTenantCallback implements DDLCallback {

		private PETenant tenant;
		private SchemaContext context;
		
		private List<CatalogEntity> updates;
		private List<CatalogEntity> deletes;
		
		public DropTenantCallback(SchemaContext sc, PETenant pet) {
			tenant = pet;
			context= sc;
		}
		
		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			return deletes;
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return SQLCommand.EMPTY;
		}

		@Override
		public boolean canRetry(Throwable t) {
			return AdaptiveMultitenantSchemaPolicyContext.canRetry(t);
		}

		@Override
		public String description() {
			return System.identityHashCode(this) + "@DropTenantCallback for " + tenant.getName();
		}

		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg) throws PEException {
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			updates = new ArrayList<CatalogEntity>();
			deletes = new ArrayList<CatalogEntity>();
			context.refresh(false);
			
			CatalogDAO c = conn.getCatalogDAO();

			Tenant onTenant = tenant.getPersistent(context);
			c.refreshForLock(onTenant);
			deletes.add(onTenant);
			for(TableVisibility tv : onTenant.getScoping()) {
				UserTable ut = tv.getTable();
				tv.setTable(null);
				updates.add(ut);
                Singletons.require(HostService.class).onGarbageEvent();
			}
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return new CacheInvalidationRecord(tenant.getCacheKey(), InvalidationScope.CASCADE);
		}

		@Override
		public boolean requiresFreshTxn() {
			return true;
		}

		@Override
		public boolean requiresWorkers() {
			// catalog only
			return false;
		}

		@Override
		public void postCommitAction(CatalogDAO c) {
			// TODO Auto-generated method stub
			
		}

		
	}
}
