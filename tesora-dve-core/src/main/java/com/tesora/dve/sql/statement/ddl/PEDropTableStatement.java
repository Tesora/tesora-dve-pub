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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.variables.VariableScope;
import com.tesora.dve.variables.Variables;
import com.tesora.dve.worker.WorkerGroup;

public class PEDropTableStatement extends
		PEDropStatement<PETable, UserTable> {

	protected Set<CatalogEntity> deletes = null;
	protected Set<CatalogEntity> updates = null;

	private List<TableKey> tableKeys;
	private List<Name> unknownTables;

	private CacheInvalidationRecord cacheInvalidationRecord = null;
	
	private boolean temporary;
	
	public PEDropTableStatement(SchemaContext sc, List<TableKey> tks, List<Name> unknownTbls, Boolean ifExists, boolean temporary) {
		super(PETable.class, ifExists, false,
				(tks.isEmpty() ? unknownTbls.get(0) : tks.get(0).getAbstractTable().getName()),
						"TABLE");
		this.tableKeys = new ArrayList<TableKey>(tks);
		this.unknownTables = unknownTbls;
		this.temporary = temporary;
		getInvalidationRecord(sc);
	}
	
	public PEDropTableStatement(PEDropTableStatement base) {
		super(PETable.class, base.isIfExists(), false, base.getTarget(), "TABLE");
		this.tableKeys = new ArrayList<TableKey>(base.tableKeys);
		this.unknownTables = new ArrayList<Name>(base.unknownTables);
		this.temporary = base.temporary;
	}

	@Override
	public PETable getTarget() {
		if (tableKeys.isEmpty()) return null;
		return tableKeys.get(0).getAbstractTable().asTable();
	}
	
	public List<TableKey> getDroppedTableKeys() {
		return tableKeys;
	}
	
	public boolean isTemporary() {
		return temporary;
	}
	
	@Override
	public PEDatabase getDatabase(SchemaContext pc) {
		if (getRoot() != null)
			return ((PETable)getRoot()).getPEDatabase(pc);
		return super.getDatabase(pc);
	}

	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		compute(pc,false);
		return new ArrayList<CatalogEntity>(updates);
	}
	
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		compute(pc,false);
		return new ArrayList<CatalogEntity>(deletes);
	}

	@Override
	protected void preplan(SchemaContext pc, ExecutionSequence es,boolean explain) throws PEException {
		if (temporary)
			return;
		super.preplan(pc, es, explain);
	}

	
	protected static void compute(SchemaContext pc, 
			Set<CatalogEntity> deletes, Set<CatalogEntity> updates, 
			List<TableKey> keys, boolean ignoreFKChecks) throws PEException {
		if (keys.isEmpty()) return;
		if (!deletes.isEmpty()) return;
		for(TableKey tk : keys) {
			PETable tab = tk.getAbstractTable().asTable();
			List<PEForeignKey> effectedForeignKeys = new ArrayList<PEForeignKey>();
			if (!tk.isUserlandTemporaryTable())
				checkForeignKeys(pc, tab, effectedForeignKeys, ignoreFKChecks);		
			pc.beginSaveContext();
			try {
				if (tk.isUserlandTemporaryTable()) {
					// for userland temp tables, we only need to toss out the temporary table record
					deletes.addAll(pc.getCatalog().findUserlandTemporaryTable(pc.getConnection().getConnectionId(),
							tab.getDatabaseName(pc).getUnquotedName().get(),
							tab.getName().getUnquotedName().get()));
				} else {
					deletes.add(tab.persistTree(pc));
					if (tab.isContainerBaseTable(pc)) {
						PEContainer cont = tab.getDistributionVector(pc).getContainer(pc);
						List<UserTable> anyTabs = pc.getCatalog().findContainerMembers(cont.getName().get());
						if (anyTabs.size() == 1) {
							// last table - this is ok
							cont.setBaseTable(pc, null);
							updates.add(cont.persistTree(pc));
							// we're also going to delete all of the container tenants
							try {
								deletes.addAll(pc.getCatalog().getDAO().findContainerTenants(cont.getPersistent(pc)));
							} catch (Exception e) {
								throw new PEException("Unable to find container tenants of container " + cont.getName(),e);
							}
						} else {
							// more than one table left - not ok
							throw new SchemaException(Pass.PLANNER, "Unable to drop table " 
									+ tab.getName().getSQL() 
									+ " because it is the base table to container " 
									+ cont.getName().getSQL() + " which is not empty");
						}
					}
					for(PEForeignKey pefk : effectedForeignKeys) 
						// this should be persisting the whole table, not the key
						updates.add(pefk.persistTree(pc));
				} 
			} finally {
				pc.endSaveContext();
			}
		}
		
	}
	
	
	protected void compute(SchemaContext pc, boolean ignoreFKChecks) throws PEException {
		if (tableKeys.isEmpty()) return;
		if (deletes != null) return; 
		deletes = new HashSet<CatalogEntity>();
		updates = new HashSet<CatalogEntity>();		
		compute(pc,deletes,updates,tableKeys,ignoreFKChecks);
	}
	
	protected static void checkForeignKeys(SchemaContext pc, PETable targetTable, 
			List<PEForeignKey> updatedKeys, boolean ignoreFKChecks) {
		boolean required = 
				Variables.FOREIGN_KEY_CHECKS.getSessionValue(pc.getConnection().getVariableSource()).booleanValue()
				&& !ignoreFKChecks;
		MultiMap<PETable, PEForeignKey> referencing = pc.findFKSReferencing(targetTable);
		if (referencing.isEmpty()) return;
		if (required) 
			throw new SchemaException(Pass.PLANNER, "Unable to drop table " + targetTable.getName().getSQL() + " because referenced by foreign keys");
		// otherwise we have to fix up the pertinent fks to use strings instead of fk refs
		for(PEForeignKey pefk : referencing.values()) {
			pefk.revertToForward(pc);
			updatedKeys.add(pefk);
		}
	}
	
	@Override
	public StatementType getStatementType() {
		return StatementType.DROP_TABLE;
	}
		
	@Override
	public boolean filterStatement(SchemaContext pc) {
		if (tableKeys.isEmpty())
			return false;
		
		for(TableKey tk : tableKeys) {
			PEAbstractTable<?> tbl = tk.getAbstractTable();
			if (tbl != null) { 
				if (pc.getConnection().isFilteredTable(
						new QualifiedName(
								tbl.getDatabase(pc).getName().getUnquotedName().getUnqualified(),
								tbl.getName().getUnquotedName().getUnqualified()))) {
					return true;
				}
			}
		}
		return false;
	}
	
	@Override
	public void plan(SchemaContext pc,ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		if (!tableKeys.isEmpty())
			es.append(buildStep(pc));
		else 
			es.append(new EmptyExecutionStep(0,"already dropped: " + getSQL(pc)));
	}
	
	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		return new ComplexDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), getRoot(), getAction(),
				new DropTableCallback(pc,getSQLCommand(pc),getInvalidationRecord(pc),
						getCatalogObjects(pc),getDeleteObjects(pc)));
	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		if (tableKeys.isEmpty())
			return null;

		if (cacheInvalidationRecord != null)
			return cacheInvalidationRecord;

		ListOfPairs<SchemaCacheKey<?>,InvalidationScope> actions = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
		for(TableKey tk : tableKeys)
			if (!tk.isUserlandTemporaryTable())
				actions.add(tk.getAbstractTable().getCacheKey(), InvalidationScope.CASCADE);
		cacheInvalidationRecord = new CacheInvalidationRecord(actions);
		
		return cacheInvalidationRecord;
	}

	private class DropTableCallback extends DDLCallback {

		private final SQLCommand command;
		private final CacheInvalidationRecord record;
		private final List<CatalogEntity> updates;
		private final List<CatalogEntity> deletes;
		private final SchemaContext context;
		
		public DropTableCallback(SchemaContext ctxt, SQLCommand theCommand, CacheInvalidationRecord theRecord,
				List<CatalogEntity> updates, List<CatalogEntity> deletes) {
			this.command = theCommand;
			this.record = theRecord;
			this.updates = updates;
			this.deletes = deletes;
			this.context = ctxt;
		}
		
		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return command;
		}

		@Override
		public String description() {
			return command.getRawSQL();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}
		
		public void postCommitAction(CatalogDAO c) throws PEException
		{
			if (unknownTables != null && unknownTables.size()>0) {
				if (!isIfExists()) {
					throw new PEException("Unknown table '" + StringUtils.join(unknownTables, ",") + "'");
				} else {
					// set warning count to number of unknown tables if we only could...
				}
			}

		}

		
		public List<CatalogEntity> getUpdatedObjects() throws PEException
		{
			return updates;
		}
		
		public List<CatalogEntity> getDeletedObjects() throws PEException
		{
			return deletes;
		}

		public void onCommit(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			for(TableKey tk : tableKeys) {
				if (tk.isUserlandTemporaryTable()) {
					wg.clearPinned();
					context.getTemporaryTableSchema().removeTable(context, (ComplexPETable) tk.getAbstractTable());
					if (context.getTemporaryTableSchema().isEmpty())
						conn.releaseInflightTemporaryTablesLock();
				}
			}
		}
				
	}
	
}
