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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.ViewMode;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.ExecutionState;
import com.tesora.dve.queryplan.QueryStepAddGenerationOperation;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEPriviledge;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.PEView;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.RangeDistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.WorkerGroup;

public class AddStorageSiteStatement extends PEAlterStatement<PEPersistentGroup> {

	protected List<PEStorageSite> sites;
	protected final boolean rebalance;
	
	public AddStorageSiteStatement(PEPersistentGroup target, List<PEStorageSite> pess, boolean rebalance) {
		super(target, true);
		sites = pess;
		this.rebalance = rebalance;
	}

	public List<PEStorageSite> getStorageSites() {
		return sites;
	}
	
	@Override
	protected PEPersistentGroup modify(SchemaContext sc, PEPersistentGroup in) {
		// don't modify
		// in.addSite(site);
		return in;
	}

	@Override
	public void normalize(SchemaContext sc) {
		super.normalize(sc);
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		normalize(sc);
		es.append(new ComplexDDLExecutionStep(null,getTarget(), getTarget(), Action.ALTER,
				new AddStorageGenCallback((PEPersistentGroup)getTarget(),sites)));
		if (rebalance) {
			StartRebalanceStatement reb = new StartRebalanceStatement(getTarget());
			reb.plan(sc, es, config);
		}
	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return null;
	}

	public static void sortTablesOnGroup(SchemaContext sc, PEPersistentGroup onGroup,
			MultiMap<PEDatabase,PETable> nonFKTables,
			MultiMap<PEDatabase,PETable> fkTables,
			MultiMap<PEDatabase,PEViewTable> viewTables) {
		for(PEAbstractTable<?> peat : sc.findTablesOnGroup(onGroup.getName().getUnquotedName().get())) {
			if (peat.isView()) {
				viewTables.put(peat.getPEDatabase(sc),peat.asView());
			} else {
				PETable pet= peat.asTable();
				if (pet.getForeignKeys(sc).isEmpty()) {
					nonFKTables.put(pet.getPEDatabase(sc),pet);
				} else {
					fkTables.put(pet.getPEDatabase(sc), pet);
				}
			}
		}
	}

	public static Pair<List<PEAbstractTable<?>>,Boolean> buildTableDeclOrder(SchemaContext sc, MultiMap<PEDatabase,PETable> nonFKTables,
			MultiMap<PEDatabase,PETable> fkTables) {
		ListSet<PEAbstractTable<?>> out = new ListSet<PEAbstractTable<?>>();
		for(PEDatabase ped : nonFKTables.keySet()) {
			Collection<PETable> sub = nonFKTables.get(ped);
			out.addAll(sub);
		}

		boolean ignore = false;
		while(fkTables.values().size() != 0) {
			int before = fkTables.values().size();
			for(Iterator<PEDatabase> dbiter = fkTables.keySet().iterator(); dbiter.hasNext();) {
				PEDatabase db = dbiter.next();
				Collection<PETable> sub = fkTables.get(db);
				for(Iterator<PETable> iter = sub.iterator(); iter.hasNext();) {
					PETable pet = iter.next();
					boolean all = true;
					if (!ignore) {
						for(PEForeignKey pefk : pet.getForeignKeys(sc)) {
							PETable ref = pefk.getTargetTable(sc);
							if (!out.contains(ref)) {
								all = false;
								break;
							}
						}
					} else {
						// final pass
						all = true;
					}
					if (all) {
						out.add(pet);
						iter.remove();
					}
				}
				if (sub.isEmpty())
					dbiter.remove();
			}
			int after = fkTables.values().size();
			if (after == before && after > 0)
				ignore = true;
		}
		return new Pair<List<PEAbstractTable<?>>,Boolean>(out,ignore);		
	}
	
	
	private static class AddStorageGenCallback extends NestedOperationDDLCallback {

		private final PEPersistentGroup theGroup;
		private final List<PEStorageSite> theSites;
		private QueryStepAddGenerationOperation op;
		private final int versionAtPlanning;
		
		public AddStorageGenCallback(PEPersistentGroup group, List<PEStorageSite> sites) {
			theGroup = group;
			theSites = sites;
			versionAtPlanning = group.getVersion();
			op = null;
		}
		
		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg)
				throws PEException {
			// this can get executed multiple times - so make sure what you do here can handle that
			op = null;
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			// make sure that the latest gen on the storage group isn't already set
			SchemaContext sc = SchemaContext.makeMutableIndependentContext(conn.getSchemaContext());
			// get persistent copies of everything
			PersistentGroup pg = null;
			List<PersistentSite> sites = new ArrayList<PersistentSite>();
			sc.beginSaveContext();
			try {
				pg = theGroup.persistTree(sc,true);
				if (pg.getLastGen().getVersion() != versionAtPlanning) {
					// no op
				} else {
					for(PEStorageSite pess : theSites)
						sites.add(pess.persistTree(sc,true));
					op = buildOperation(sc, pg, sites);
				}
			} finally {
				sc.endSaveContext();
			}
		}

		// find all the databases on the persistent group
		// load all the tables in the databases
		// sort into three groups:
		// [1] plain - no fks, emulated view
		// [2] fks
		// [3] pushdown view
		// we know we can emit all of [1] right away
		// we have to build a dep tree for [2] - we can figure out here if it's cyclical
		// we have to build a dep tree for [3]
		private QueryStepAddGenerationOperation buildOperation(SchemaContext sc, PersistentGroup pg, List<PersistentSite> sites) throws PEException {
			MultiMap<PEDatabase,PETable> plain = new MultiMap<PEDatabase,PETable>();
			MultiMap<PEDatabase,PETable> fks = new MultiMap<PEDatabase,PETable>();
			MultiMap<PEDatabase,PEViewTable> views = new MultiMap<PEDatabase,PEViewTable>();
			sortTablesOnGroup(sc,theGroup,plain,fks,views);			
			ListSet<PEDatabase> dbs = new ListSet<PEDatabase>();
			dbs.addAll(views.keySet());
			dbs.addAll(plain.keySet());
			dbs.addAll(fks.keySet());
			ListOfPairs<UserTable,SQLCommand> commands = new ListOfPairs<UserTable,SQLCommand>(
					plain.values().size() + fks.values().size());
			ListSet<PEAbstractTable<?>> emitted = new ListSet<PEAbstractTable<?>>();
			Pair<List<PEAbstractTable<?>>,Boolean> tableDeclOrder = buildTableDeclOrder(sc,plain,fks);
			emitted.addAll(tableDeclOrder.getFirst());
			MultiMap<RangeDistribution,PETable> tablesByRange = new MultiMap<RangeDistribution,PETable>();
			for(PEAbstractTable<?> abst : tableDeclOrder.getFirst()) {
				if (abst.isView()) continue;
				PETable pet = abst.asTable();
				commands.add(pet.getPersistent(sc),getCommand(sc,new PECreateTableStatement(pet,false)));
				if (pet.getDistributionVector(sc).isRange()) {
					RangeDistributionVector rdv = (RangeDistributionVector) pet.getDistributionVector(sc);
					tablesByRange.put(rdv.getRangeDistribution().getDistribution(sc),pet);
				}
			}
			emitViews(sc,views,commands, emitted);
			List<SQLCommand> users = new ArrayList<SQLCommand>();
			emitUsers(sc,dbs, users);
			/*
			for(Pair<UserTable,SQLCommand> p : commands) {
				System.out.println(p.getFirst());
				System.out.println("  " + p.getSecond().getRawSQL());
			}
			for(SQLCommand s : users) {
				System.out.println(s.getRawSQL());
			}
			*/
			return new QueryStepAddGenerationOperation(pg,sites,null,commands,tableDeclOrder.getSecond(),users);
		}

		private void emitViews(SchemaContext sc, MultiMap<PEDatabase,PEViewTable> views, ListOfPairs<UserTable,SQLCommand> commands,
				ListSet<PEAbstractTable<?>> emitted) {
			HashMap<PEViewTable,ProjectingStatement> unresolved = new HashMap<PEViewTable,ProjectingStatement>();
			while(views.values().size() != 0) {
				int wedged = views.values().size();
				for(Iterator<PEDatabase> dbiter = views.keySet().iterator(); dbiter.hasNext();) {
					PEDatabase db = dbiter.next();
					Collection<PEViewTable> sub = views.get(db);
					for(Iterator<PEViewTable> iter = sub.iterator(); iter.hasNext();) {
						PEViewTable pevt = iter.next();
						PEView pev = pevt.getView(sc);
						if (pev.getMode() == ViewMode.EMULATE) {
							// add a table def
							commands.add(pevt.getPersistent(sc), new SQLCommand(sc, pevt.getDeclaration()));
							iter.remove();
							emitted.add(pevt);
						} else {
							ProjectingStatement ps = unresolved.get(pevt);
							if (ps == null) 
								ps = pevt.getView(sc).getViewDefinition(sc,pevt,false);
							ListSet<TableKey> allTabs = ps.getDerivedInfo().getAllTableKeys();
							boolean all = true; // assume
							for(TableKey tk : allTabs) {
								if (!emitted.contains(tk.getAbstractTable())) {
									all = false;
									break;
								}
							}
							if (all) {
								commands.add(pevt.getPersistent(sc),getCommand(sc,new PECreateViewStatement(sc,true,pevt)));
								iter.remove();
								emitted.add(pevt);
							} else {
								unresolved.put(pevt,ps);
							}
						}
					}
					if (sub.isEmpty())
						dbiter.remove();
				}
				int now = views.values().size();
				if (wedged == now && now != 0)
					throw new SchemaException(Pass.PLANNER, "Unable to compute view declaration order");
			}
			
		}

		private void emitUsers(SchemaContext sc, ListSet<PEDatabase> dbs, List<SQLCommand> commands) {
			MultiMap<PEUser,PEPriviledge> privs = sc.findUsersForGenAdd();
			for(PEUser peu : privs.keySet()) {
				commands.add(getCommand(sc, new PECreateUserStatement(Collections.singletonList(peu),false)));
				List<SQLCommand> grants = new ArrayList<SQLCommand>();
				PEPriviledge global = null;
				for(PEPriviledge priv : privs.get(peu)) {
					if (priv.isGlobal()) {
						global = priv;
						break;
					}
					if (priv.getDatabase() != null) {
						grants.add(getCommand(sc,new GrantStatement(priv)));						
					} else {
						// tenant grant - just grant on the underlying database
						grants.add(getCommand(sc,new GrantStatement(new PEPriviledge(peu,priv.getTenant().getDatabase(sc)))));
					}
				}
				if (global != null) {
					commands.add(getCommand(sc,new GrantStatement(global)));
				} else {
					commands.addAll(grants);
				}
			}
		}
		
		private SQLCommand getCommand(SchemaContext sc, Statement s) {
			EmitOptions opts = EmitOptions.NONE.addQualifiedTables();
	        GenericSQLCommand gsql = s.getGenericSQL(sc, Singletons.require(HostService.class).getDBNative().getEmitter(), opts);
			return gsql.resolve(sc,null).getSQLCommand();			
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		@Override
		public String description() {
			return "AddStorageGeneration";
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return CacheInvalidationRecord.GLOBAL;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

		@Override
		public void postCommitAction(CatalogDAO c) throws PEException {
			// could release the x lock here I guess.
		}

		@Override
		public void executeNested(ExecutionState estate, WorkerGroup wg,
				DBResultConsumer resultConsumer) throws Throwable {
			if (op == null) return;
			op.executeSelf(estate, wg, resultConsumer);
		}
		
	}

}
