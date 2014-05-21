// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.ViewMode;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepAddGenerationOperation;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
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
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.worker.WorkerGroup;

public class AddStorageSiteStatement extends PEAlterStatement<PEPersistentGroup> {

	protected List<PEStorageSite> sites;
	
	public AddStorageSiteStatement(PEPersistentGroup target, List<PEStorageSite> pess) {
		super(target, true);
		sites = pess;
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
	protected ExecutionStep buildStep(SchemaContext sc) throws PEException {
		return new ComplexDDLExecutionStep(null,getTarget(), getTarget(), Action.ALTER,
				new AddStorageGenCallback((PEPersistentGroup)getTarget(),sites));
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return null;
	}

	private static class AddStorageGenCallback implements NestedOperationDDLCallback {

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
		private QueryStepAddGenerationOperation buildOperation(SchemaContext sc, PersistentGroup pg, List<PersistentSite> sites) {
			MultiMap<PEDatabase,PETable> plain = new MultiMap<PEDatabase,PETable>();
			MultiMap<PEDatabase,PETable> fks = new MultiMap<PEDatabase,PETable>();
			MultiMap<PEDatabase,PEViewTable> views = new MultiMap<PEDatabase,PEViewTable>();
			List<PEAbstractTable<?>> allTabs = sc.findTablesOnGroup(pg.getName());
			for(PEAbstractTable<?> peat : sc.findTablesOnGroup(pg.getName())) {
				if (peat.isView()) {
					views.put(peat.getPEDatabase(sc),peat.asView());
				} else {
					PETable pet= peat.asTable();
					if (pet.getForeignKeys(sc).isEmpty()) {
						plain.put(pet.getPEDatabase(sc),pet);
					} else {
						fks.put(pet.getPEDatabase(sc), pet);
					}
				}
			}
			ListSet<PEDatabase> dbs = new ListSet<PEDatabase>();
			dbs.addAll(views.keySet());
			dbs.addAll(plain.keySet());
			dbs.addAll(fks.keySet());
			ListOfPairs<UserTable,SQLCommand> commands = new ListOfPairs<UserTable,SQLCommand>(allTabs.size());
			ListSet<PEAbstractTable<?>> emitted = new ListSet<PEAbstractTable<?>>();
			emitPlainTables(sc,plain,commands, emitted);
			boolean ignoreFKs = emitForeignKeyTables(sc,fks,commands, emitted);
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
			return new QueryStepAddGenerationOperation(pg,sites,null,commands,ignoreFKs,users);
		}

		private void emitPlainTables(SchemaContext sc, MultiMap<PEDatabase,PETable> tabs, ListOfPairs<UserTable,SQLCommand> commands,
				ListSet<PEAbstractTable<?>> emitted) {
			for(PEDatabase ped : tabs.keySet()) {
				Collection<PETable> sub = tabs.get(ped);
				emitted.addAll(sub);
				for(PETable pet : sub) 
					commands.add(pet.getPersistent(sc),getCommand(sc,new PECreateTableStatement(pet,false)));
			}
		}
		
		private boolean emitForeignKeyTables(SchemaContext sc, MultiMap<PEDatabase,PETable> tabs, ListOfPairs<UserTable,SQLCommand> commands,
				ListSet<PEAbstractTable<?>> emitted) {
			boolean ignore = false;
			while(tabs.values().size() != 0) {
				int before = tabs.values().size();
				for(Iterator<PEDatabase> dbiter = tabs.keySet().iterator(); dbiter.hasNext();) {
					PEDatabase db = dbiter.next();
					Collection<PETable> sub = tabs.get(db);
					for(Iterator<PETable> iter = sub.iterator(); iter.hasNext();) {
						PETable pet = iter.next();
						boolean all = true;
						if (!ignore) {
							for(PEForeignKey pefk : pet.getForeignKeys(sc)) {
								PETable ref = pefk.getTargetTable(sc);
								if (!emitted.contains(ref)) {
									all = false;
									break;
								}
							}
						} else {
							// final pass
							all = true;
						}
						if (all) {
							commands.add(pet.getPersistent(sc),getCommand(sc,new PECreateTableStatement(pet,false)));
							emitted.add(pet);
							iter.remove();
						}
					}
					if (sub.isEmpty())
						dbiter.remove();
				}
				int after = tabs.values().size();
				if (after == before && after > 0)
					ignore = true;
			}
			return ignore;
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
							commands.add(pevt.getPersistent(sc),new SQLCommand(pevt.getDeclaration()));
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

		private void emitUsers(SchemaContext sc,ListSet<PEDatabase> dbs, List<SQLCommand> commands) {
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
			GenericSQLCommand gsql = s.getGenericSQL(sc, false, false);
			return gsql.resolve(sc,null).getSQLCommand();			
		}
		
		
		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			// the operation takes care of this itself
			return Collections.emptyList();
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			return Collections.emptyList();
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return SQLCommand.EMPTY;
		}

		@Override
		public boolean canRetry(Throwable t) {
			// let's just say we can't.
			return false;
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
		public void executeNested(SSConnection conn, WorkerGroup wg,
				DBResultConsumer resultConsumer) throws Throwable {
			if (op == null) return;
			op.execute(conn, wg, resultConsumer);
		}

		@Override
		public void prepareNested(SSConnection conn, CatalogDAO c,
				WorkerGroup wg, DBResultConsumer resultConsumer)
				throws PEException {
			// TODO Auto-generated method stub
			
		}
		
	}

}
