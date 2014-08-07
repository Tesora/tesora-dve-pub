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

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEForeignKeyColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEKeyColumnBase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils;
import com.tesora.dve.sql.schema.mt.FKRefMaintainer;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CreateTableOperation;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CreateTenantScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.LateFKFixup;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CompositeNestedOperation;
import com.tesora.dve.sql.schema.mt.PETenant.TenantCacheKey;
import com.tesora.dve.sql.schema.validate.ForeignKeyValidateResult;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variables.KnownVariables;

// creating a tenant table
// for all cases we need to add a tvr
// for landlord tenant/private table we need to add the sql and user table
public class PECreateTenantTableStatement extends PECreateTableStatement {
	
	private PETenant onTenant;
	private UnqualifiedName logicalName;
	private Long autoIncStart;
		
	public PECreateTenantTableStatement(PECreateTableStatement basedOn, PETenant forTenant, Long autoIncOffset, UnqualifiedName logicalName) {
		super(basedOn);
		onTenant = forTenant;
		this.logicalName = logicalName;
		autoIncStart = autoIncOffset;
	}
	
	public PETenant getTenant() {
		return onTenant;
	}
	
	public UnqualifiedName getLogicalName() {
		return logicalName;
	}
	
	protected void checkColocatedFKs(SchemaContext sc) {
		PETable tab = getTable();
		if (sc.isPersistent()) {
			List<ValidateResult> results = tab.validate(sc,false);
			// make sure we fail on errors
			for(ValidateResult vr : results) {
				if (vr.isError()) {
					String message = null;
					if (vr instanceof ForeignKeyValidateResult) {
						// need to map the tables
						HashMap<PETable,UnqualifiedName> mapping = new HashMap<PETable,UnqualifiedName>();
						mapping.put(tab,logicalName);
						PEForeignKey pefk = (PEForeignKey) vr.getSubject();
						if (pefk.getTable(sc) != tab) {
							TableScope theScope = sc.getPolicyContext().getOfTenant(pefk.getTable(sc).getPersistent(sc));
							mapping.put(pefk.getTable(sc),theScope.getName().getUnqualified());
						} 
						if (pefk.getTargetTable(sc) != tab) {
							TableScope theScope = sc.getPolicyContext().getOfTenant(pefk.getTargetTable(sc).getPersistent(sc));
							mapping.put(pefk.getTargetTable(sc),theScope.getName().getUnqualified());
						}
						ForeignKeyValidateResult fkvr = (ForeignKeyValidateResult) vr;
						fkvr.setMTMapping(mapping);
						message = fkvr.getMessage(sc);
					} else {
						message = vr.getMessage(sc);
					}
					throw new SchemaException(Pass.NORMALIZE,message);
				}
			}
		}
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config)
			throws PEException {
		PETable tab = getTable();
		// this is probably completely unecessary if the table is well formed and already exists
		checkColocatedFKs(sc);
		Map<UnqualifiedName,LateFKFixup> setNullFixups = new HashMap<UnqualifiedName,LateFKFixup>();
		AdaptiveMTDDLPlannerUtils.handleSetNullActions(sc, es, tab, null, onTenant, setNullFixups);
		boolean wellFormed = AdaptiveMTDDLPlannerUtils.isWellFormed(sc, tab, null);
		CompositeNestedOperation uno = new CompositeNestedOperation();
		CreateTableOperation ncto =
				AdaptiveMTDDLPlannerUtils.addCreateTable(sc,es,onTenant,tab,logicalName,
						(wellFormed? TableState.SHARED : TableState.FIXED), setNullFixups);
		uno.withChange(new CreateTenantScope((TenantCacheKey)onTenant.getCacheKey(), logicalName, autoIncStart, ncto));
		TableScope ts = new TableScope(sc, tab, onTenant, autoIncStart, logicalName);
		CreateTableFKRefMaintainer disaster = 
				new CreateTableFKRefMaintainer(onTenant,tab,logicalName,ts,ncto);
		disaster.maintain(sc);
		disaster.schedule(sc, es, uno);
		AdaptiveMTDDLPlannerUtils.addDDLCallback(sc, tab.getPEDatabase(sc), tab.getPersistentStorage(sc), tab, Action.CREATE, es, uno, null);
	}
	
	protected MultiMap<TableScope, PEForeignKey> computeRootSet(SchemaContext sc, PETable target) {
		boolean required = 
				KnownVariables.FOREIGN_KEY_CHECKS.getSessionValue(sc.getConnection().getVariableSource()).booleanValue();
		// we may have just resolved a foreign key - see if that is the case
		List<TableScope> matching = sc.findScopesWithUnresolvedFKsTargeting(onTenant.getDatabase(sc).getName().getUnqualified(), logicalName, onTenant);
		// first find the set that matches - that will be the root set of the schema graph
		MultiMap<TableScope,PEForeignKey> rootSet = new MultiMap<TableScope,PEForeignKey>();
		for(TableScope ts : matching) {
			PETable backing = ts.getTable(sc);
			for(PEKey pek : backing.getKeys(sc)) {
				if (!pek.isForeign()) continue;
				PEForeignKey pefk = (PEForeignKey) pek;
				if (!pefk.isForward()) continue;
				if (pefk.getTargetTableName(sc).equals(logicalName)) {
					for(PEKeyColumnBase pekc : pefk.getKeyColumns()) {
						PEForeignKeyColumn pefkc = (PEForeignKeyColumn)pekc;
						PEColumn tc = target.lookup(sc, pefkc.getTargetColumnName());
						if (tc == null && required) {
							// should we make this conditional on the var?  probably
							throw new SchemaException(Pass.NORMALIZE, 
									"No such column: " + pefkc.getTargetColumnName() 
									+ " in table " + logicalName 
									+ " required for foreign key in table " + ts.getName());
						}
					}
					rootSet.put(ts, pefk);
				}
			}
		}
		return rootSet;
	}
			
	public static class CreateTableFKRefMaintainer extends FKRefMaintainer {

		protected PETable newTab;
		protected UnqualifiedName logicalName;
		protected TableScope theScope;
		
		public CreateTableFKRefMaintainer(PETenant onTenant, PETable nt, UnqualifiedName nn, TableScope ns, CreateTableOperation newCreate) {
			super(onTenant);
			newTab = nt;
			logicalName = nn;
			theScope = ns;
			forwarding.put(nt,newCreate);
		}

		@Override
		protected void createBlocks(SchemaContext sc) {
			modifications.put(newTab, new ModificationBlock(sc,theScope,newTab));
			super.createBlocks(sc);
		}
		
		@Override
		public ListOfPairs<TableScope, TaggedFK> computeRootSet(SchemaContext sc) {
			boolean required = 
					KnownVariables.FOREIGN_KEY_CHECKS.getSessionValue(sc.getConnection().getVariableSource()).booleanValue();
			// we may have just resolved a foreign key - see if that is the case
			List<TableScope> matching = sc.findScopesWithUnresolvedFKsTargeting(tenant.getDatabase(sc).getName().getUnqualified(), logicalName, tenant);
			ListOfPairs<TableScope, TaggedFK> out = new ListOfPairs<TableScope, TaggedFK>();
			for(TableScope ts : matching) {
				PETable backing = ts.getTable(sc);
				for(PEKey pek : backing.getKeys(sc)) {
					if (!pek.isForeign()) continue;
					PEForeignKey pefk = (PEForeignKey) pek;
					if (!pefk.isForward()) continue;
					if (pefk.getTargetTableName(sc).equals(logicalName)) {
						for(PEKeyColumnBase pekc : pefk.getKeyColumns()) {
							PEForeignKeyColumn pefkc = (PEForeignKeyColumn)pekc;
							PEColumn tc = newTab.lookup(sc, pefkc.getTargetColumnName());
							if (tc == null && required) {
								// TODO: should we make this conditional on the var
								throw new SchemaException(Pass.NORMALIZE, 
										"No such column: " + pefkc.getTargetColumnName() 
										+ " in table " + logicalName 
										+ " required for foreign key in table " + ts.getName());
							}
						}
						out.add(theScope, new TaggedFK(pefk,backing,ts));
					}
				}
			}
			return out;
		}

		@Override
		public void modifyRoot(SchemaContext sc, Pair<TableScope, TaggedFK> r,
				Map<PETable, ModificationBlock> blocks) {
			PETable target = r.getFirst().getTable(sc);
			PEForeignKey pefk = r.getSecond().getFK();
			Pair<PEForeignKey,PEKey> nk = AdaptiveMTDDLPlannerUtils.maybeRequiresNewKey(sc, pefk, target);
			if (nk != null) {
				target.addKey(sc, nk.getSecond(), false);
				target.setDeclaration(sc, target);
			}
			ModificationBlock mb = blocks.get(target);
			ModificationBlock emb = blocks.get(r.getSecond().getEnclosing());
			emb.resolveFK(sc, blocks, pefk, mb);
		}
		
	}
	
}
