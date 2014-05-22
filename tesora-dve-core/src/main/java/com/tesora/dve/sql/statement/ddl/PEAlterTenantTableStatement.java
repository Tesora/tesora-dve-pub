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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils;
import com.tesora.dve.sql.schema.mt.FKRefMaintainer;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CreateTableOperation;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.DirectAllocatedTable;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.ExecuteAlters;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.LateFKFixup;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.LazyAllocatedTable;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.TableFlip;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CompositeNestedOperation;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class PEAlterTenantTableStatement extends PEAlterTableStatement {

	private final TableScope scope;
	private final SchemaCacheKey<PETenant> tenantKey;
	// so we can get it from elsewhere after planning is done
	LazyAllocatedTable targetTableName;
	
	@SuppressWarnings("unchecked")
	public PEAlterTenantTableStatement(SchemaContext sc, PEAlterTableStatement orig, TableScope scope, PETenant tenant) {
		super(sc, orig.targetTableKey,orig.getActions());
		this.scope = scope;
		this.tenantKey = (SchemaCacheKey<PETenant>) tenant.getCacheKey();
		this.targetTableName = null;
	}

	private PETable getTable() {
		return getTableKey().getAbstractTable().asTable();
	}

	public LazyAllocatedTable getFinalTableName() {
		return targetTableName;
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		PETable subjectTable = getTable();
		PETable newCopy = subjectTable.recreate(sc, getTable().getDeclaration(), new LockInfo(LockType.EXCLUSIVE, "alter tenant table"));
		alterTable(sc, newCopy, null);
		newCopy.setDeclaration(sc, newCopy);
		HashMap<UnqualifiedName,LateFKFixup> fixups = new HashMap<UnqualifiedName,LateFKFixup>();
		AdaptiveMTDDLPlannerUtils.handleSetNullActions(sc, es, newCopy, scope, scope.getTenant(sc), fixups);
		boolean wellFormed = AdaptiveMTDDLPlannerUtils.isWellFormed(sc, newCopy,null);
		CompositeNestedOperation uno = new CompositeNestedOperation();
		CreateTableOperation cto = null;
		if (wellFormed || getTable().getState() == TableState.SHARED) {
			cto = 
					AdaptiveMTDDLPlannerUtils.addCreateTable(sc, es, scope.getTenant(sc), 
							newCopy, scope.getName().getUnqualified(),
							(wellFormed ? TableState.SHARED : TableState.FIXED), fixups);
			TableFlip tf = new TableFlip(scope,(TableCacheKey)getTable().getCacheKey(),cto,true,new CacheInvalidationRecord(tenantKey,InvalidationScope.CASCADE));
			sc.getConnection().acquireLock(scope.getLock("flip due to alter",sc), LockType.EXCLUSIVE);
			uno.withChange(tf);
			targetTableName = tf;
		} else {
			// not well formed and not shared - alter in place
			targetTableName = new DirectAllocatedTable((TableCacheKey) getTable().getCacheKey(),wellFormed);
			uno.withChange(new ExecuteAlters(this,true,targetTableName));			
		}
		AlterTableFKRefMaintainer maintainer = new AlterTableFKRefMaintainer(scope.getTenant(sc),subjectTable,scope,cto);
		maintainer.maintain(sc);
		maintainer.schedule(sc, es, uno);
		AdaptiveMTDDLPlannerUtils.addDDLCallback(sc, subjectTable.getPEDatabase(sc), subjectTable.getPersistentStorage(sc), 
				subjectTable, Action.ALTER, es, uno, null);
	}

	private static class AlterTableFKRefMaintainer extends FKRefMaintainer {

		protected PETable subjectTable;
		protected TableScope subjectScope;
		protected ModificationBlock nb;
		
		public AlterTableFKRefMaintainer(PETenant onTenant, PETable tab, TableScope scope, CreateTableOperation cto) {
			super(onTenant);
			subjectTable = tab;
			subjectScope = scope;
			if (cto != null)
				forwarding.put(tab,cto);
			nb = null;
		}

		@Override
		public ListOfPairs<TableScope, TaggedFK> computeRootSet(SchemaContext sc) {
			// our root set is everything that refers to us
			List<TableScope> yonScopes = sc.findScopesForFKTargets(subjectTable, tenant);
			ListOfPairs<TableScope,TaggedFK> out = new ListOfPairs<TableScope,TaggedFK>();
			for(TableScope ts : yonScopes) {
				PETable yonTable = ts.getTable(sc);
				for(PEKey pek : yonTable.getKeys(sc)) {
					if (!pek.isForeign()) continue;
					PEForeignKey pefk = (PEForeignKey) pek;
					if (pefk.isForward()) continue;
					if (pefk.getTargetTable(sc).getCacheKey().equals(subjectTable.getCacheKey())) {
						out.add(subjectScope, new TaggedFK(pefk,yonTable,ts));
					}
				}
			}
			return out;			
		}

		@Override
		public void modifyRoot(SchemaContext sc, Pair<TableScope, TaggedFK> r,
				Map<PETable, ModificationBlock> blocks) {
			// we're going to put in a new mod block for the new definition
			if (nb == null) {
				nb = new ModificationBlock(sc,subjectScope,subjectTable);
				nb.buildInitialState(sc, blocks);
				blocks.put(subjectTable, nb);
			}
			ModificationBlock mb = blocks.get(r.getSecond().getEnclosing());
			mb.resolveFK(sc, blocks, r.getSecond().getFK(), nb);
		}
		
	}
}
