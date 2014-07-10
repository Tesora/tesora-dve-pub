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
import java.util.Map;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Shape;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DatabaseLock;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.schema.mt.FKRefMaintainer;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.DropTenantScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CompositeNestedOperation;
import com.tesora.dve.sql.schema.mt.TableScope.ScopeCacheKey;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variables.VariableScope;
import com.tesora.dve.variables.Variables;
import com.tesora.dve.worker.WorkerGroup;

public class PEDropTenantTableStatement extends PEDropTableStatement {

	private TableScope scope;
	private PETenant tenant;
	
	public PEDropTenantTableStatement(PEDropTableStatement basedOn, TableScope ts, PETenant tenant) {
		super(basedOn);
		scope = ts;
		this.tenant = tenant;
	}

	// was well formed		is now well formed		action
	// yes					yes						can't happen
	// yes					no						flip to fixed
	// no					no						alter (make the referencing fk be forward)
	// no					yes						flip to shared
	// the notable exception is if the tenant is the landlord and the scope is null
	// in that case xlock the shape, figure out whether the table is still referenced, then drop it if possible.
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		if (tenant == null) {
			planLandlordDropTable(sc,es);
		} else {
			planTenantDropTable(sc,es);
		}
	}
	
	private void planLandlordDropTable(SchemaContext sc, ExecutionSequence es) throws PEException {
		TableKey tk = getDroppedTableKeys().get(0);
		PETable theTable = tk.getAbstractTable().asTable();

		sc.beginSaveContext();
		UserTable ut = null;
		try {
			ut = theTable.getPersistent(sc);
		} finally {
			sc.endSaveContext();
		}

		DatabaseLock dl = new DatabaseLock("landlord drop table " + tk.getCacheKey(),theTable.getPEDatabase(sc));
		sc.getConnection().acquireLock(dl, LockType.WSHARED);

		Shape s = ut.getShape();
		ShapeLock sl = new ShapeLock("landlord drop table " + tk.getCacheKey(), s.getDatabase().getName(),s.getName(),s.getTypeHash());
		// have to use exclusive to avoid tossing it underneath an ongoing operation
		sc.getConnection().acquireLock(sl, LockType.EXCLUSIVE);
		es.append(new ComplexDDLExecutionStep(theTable.getPEDatabase(sc),theTable.getStorageGroup(sc),theTable,Action.DROP,
				new DropTenantTableCallback(this,getSQLCommand(sc))));
	}

	// regular tenant drop, rules for referencing tables.
	// was well formed		is now well formed		action
	// yes					yes						can't happen
	// yes					no						flip to fixed
	// no					no						alter (make the referencing fk be forward)
	// no					yes						flip to shared
	private void planTenantDropTable(SchemaContext sc, ExecutionSequence es) throws PEException {
		// note that because the table garbage collector is responsible for killing off tables we don't do it
		// so in the event that the subject table has no referring tables, this is a quick catalog update to
		// remove the scope from the scopes table
		PETable backingTable = scope.getTable(sc);
		CompositeNestedOperation uno = new CompositeNestedOperation();
		uno.withChange(new DropTenantScope((ScopeCacheKey) scope.getCacheKey()));
		DropTableFKRefMaintainer maintenance = new DropTableFKRefMaintainer(tenant, backingTable, scope);
		maintenance.maintain(sc);
		maintenance.schedule(sc, es, uno);
		AdaptiveMTDDLPlannerUtils.addDDLCallback(sc, backingTable.getPEDatabase(sc), backingTable.getPersistentStorage(sc), 
				backingTable, Action.CREATE, es, uno, 0L);
	}
		
	private static class DropTableFKRefMaintainer extends FKRefMaintainer {

		protected PETable subjectTable;
		protected TableScope subjectScope;
		
		public DropTableFKRefMaintainer(PETenant onTenant, PETable toDrop, TableScope theScope) {
			super(onTenant);
			subjectTable = toDrop;
			subjectScope = theScope;
		}

		@Override
		public ListOfPairs<TableScope, TaggedFK> computeRootSet(SchemaContext sc) {
			boolean required = 
					Variables.FOREIGN_KEY_CHECKS.getSessionValue(sc.getConnection().getVariableSource()).booleanValue();
			// our root set is everything that refers to us
			List<TableScope> yonScopes = sc.findScopesForFKTargets(subjectTable, tenant);
			ListOfPairs<TableScope, TaggedFK> out = new ListOfPairs<TableScope, TaggedFK>();
			for(TableScope ts : yonScopes) {
				PETable yonTable = ts.getTable(sc);
				for(PEKey pek : yonTable.getKeys(sc)) {
					if (!pek.isForeign()) continue;
					PEForeignKey pefk = (PEForeignKey) pek;
					if (pefk.isForward()) continue;
					if (pefk.getTargetTable(sc).getCacheKey().equals(subjectTable.getCacheKey())) {
						if (required) 
							throw new SchemaException(Pass.PLANNER,
									"Unable to drop table " + subjectScope.getName().getQuoted() + " because referenced by foreign keys");
						out.add(subjectScope,new TaggedFK(pefk,yonTable,ts));
					}
				}
			}
			return out;
		}

		@Override
		public void modifyRoot(SchemaContext sc, Pair<TableScope, TaggedFK> r,
				Map<PETable, ModificationBlock> blocks) {
			PEForeignKey pefk = r.getSecond().getFK();
			PETable enc = r.getSecond().getEnclosing();
			ModificationBlock mb = blocks.get(enc);
			mb.unresolveFK(pefk, r.getFirst().getName().getUnqualified());
		}
		
	}
	
	private static class DropTenantTableCallback extends DDLCallback {

		private final TableKey toDrop;
		private final String tableName;
		private final String databaseName;
		private final ListSet<CatalogEntity> deletedEntities;
		private final ListSet<CatalogEntity> updatedEntities;
		private final CacheInvalidationRecord record;
		private final SQLCommand sql;
		
		public DropTenantTableCallback(PEDropTenantTableStatement orig, SQLCommand sql) {
			this.toDrop = orig.getDroppedTableKeys().get(0);
			this.tableName = toDrop.getAbstractTable().getName().getUnquotedName().get();
			this.databaseName = ((TableCacheKey)toDrop.getAbstractTable().getCacheKey()).getDatabaseName();
			this.deletedEntities = new ListSet<CatalogEntity>();
			this.updatedEntities = new ListSet<CatalogEntity>();
			this.record = new CacheInvalidationRecord(toDrop.getCacheKey(),InvalidationScope.CASCADE);
			this.sql = sql;
		}
		
		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg)
				throws PEException {
			deletedEntities.clear();
			updatedEntities.clear();
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			CatalogDAO c = conn.getCatalogDAO();
			// just create a new schema context
			SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();

			PETable reloaded = (PETable) sc.getSource().find(sc, toDrop.getCacheKey());
			if (reloaded == null)
				return;
			
			String command =
					"select count(*) from user_table ut inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"inner join scope s on s.scope_table_id = ut.table_id where ut.name = '%s' and ud.name = '%s'";
			
			@SuppressWarnings("rawtypes")
			List results =
					c.nativeQuery(
							String.format(command,tableName,databaseName),
							Collections.<String, Object> emptyMap());
			Object first = results.get(0);
			Number n = (Number) first;
			if (n.longValue() == 0) {
				List<TableKey> freshTKs = new ArrayList<TableKey>();
				freshTKs.add(TableKey.make(sc,reloaded,1));
				PEDropTableStatement.compute(sc, deletedEntities, updatedEntities, freshTKs, true);
			} else {
				// still has references - this becomes a noop
			}
		}

		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updatedEntities;
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			return deletedEntities;
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			if (deletedEntities.isEmpty())
				return SQLCommand.EMPTY;
			return sql;
		}

		@Override
		public boolean canRetry(Throwable t) {
			return AdaptiveMultitenantSchemaPolicyContext.canRetry(t);
		}

		@Override
		public boolean requiresFreshTxn() {
			return true;
		}

		@Override
		public String description() {
			return this.getClass().getSimpleName() + " on " + toDrop;
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

	}
	
}
