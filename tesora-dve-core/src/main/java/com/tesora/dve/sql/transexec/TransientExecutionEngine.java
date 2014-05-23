package com.tesora.dve.sql.transexec;

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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.TwoDimensionalMap;
import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.common.catalog.ContainerTenant;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.PersistentTemplate;
import com.tesora.dve.common.catalog.Priviledge;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.RawPlan;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.UserView;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.infomessage.ConnectionMessageManager;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.CatalogContext;
import com.tesora.dve.sql.schema.ConnectionContext;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEContainerTenant;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEDynamicGroup;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEProject;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.SchemaContext.PersistContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.cache.EntityCacheKey;
import com.tesora.dve.sql.schema.cache.PersistentCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.HollowTableVisibility;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.DDLStatement;
import com.tesora.dve.sql.statement.ddl.PECreateDatabaseStatement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTableStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTenantTableStatement;
import com.tesora.dve.sql.statement.ddl.PECreateViewStatement;
import com.tesora.dve.sql.statement.ddl.PEDropStatement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.session.SessionSetVariableStatement;
import com.tesora.dve.sql.statement.session.SessionStatement;
import com.tesora.dve.sql.statement.session.SetExpression;
import com.tesora.dve.sql.statement.session.SetVariableExpression;
import com.tesora.dve.sql.statement.session.UseContainerStatement;
import com.tesora.dve.sql.statement.session.UseDatabaseStatement;
import com.tesora.dve.sql.statement.session.UseStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;
import com.tesora.dve.variable.SchemaVariableConstants;
import com.tesora.dve.variable.SessionVariableHandler;
import com.tesora.dve.variable.VariableAccessor;
import com.tesora.dve.variable.VariableConfig;
import com.tesora.dve.variable.VariableInfo;
import com.tesora.dve.variable.VariableScopeKind;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

/*
 * Essentially a mock class of the full engine; used in tests, upgrader, analyzer.
 */
public class TransientExecutionEngine implements CatalogContext, ConnectionContext {

	private SchemaContext tpc;
	private List<PersistentSite> sites = new ArrayList<PersistentSite>();
	private List<PersistentGroup> groups = new ArrayList<PersistentGroup>();
	private List<UserDatabase> dbs = new ArrayList<UserDatabase>();
	private List<Project> projects = new ArrayList<Project>();
	private List<DistributionRange> rds = new ArrayList<DistributionRange>();
	private List<Tenant> tenants = new ArrayList<Tenant>();
	private List<PersistentTemplate> templates = new ArrayList<PersistentTemplate>();
	private Map<String, DistributionModel> models;
	private Map<UserTable, Long> autoincs = new HashMap<UserTable, Long>();
	private MultiMap<String, TableVisibility> scopes = new MultiMap<String, TableVisibility>();
	private Map<UserTable,RangeTableRelationship> rangesForTables = new HashMap<UserTable,RangeTableRelationship>();
	private List<Container> containers = new ArrayList<Container>();
	private List<ContainerTenant> containerTenants = new ArrayList<ContainerTenant>();
	private List<UserView> views = new ArrayList<UserView>();
	private List<User> users = new ArrayList<User>();

	// single default policy
	private DynamicPolicy defaultPolicy = null;

	
	// associate a schema cache key with a catalog entity
	private Map<SchemaCacheKey<?>,CatalogEntity> cats;

	private String tempTableKern;
	private int tempTableCounter;
	
	private TwoDimensionalMap<VariableScopeKind, String, String> variables;
	
	
	private IPETenant currentTenant = null; 
	private PEUser currentUser = null;
	private Database<?> currentDatabase = null;
	
	private final ConnectionMessageManager messages = new ConnectionMessageManager();
	
	public TransientExecutionEngine(String ttkern) {
		variables = buildDefaultVariables();
		tpc = SchemaContext.createContext(this,this);
		currentUser = new PEUser(tpc);
		users.add(new User(currentUser.getUserScope().getUserName(), 
				currentUser.getPassword(),
				currentUser.getUserScope().getScope(),
				true));
		models = new HashMap<String, DistributionModel>();
		models.put(BroadcastDistributionModel.MODEL_NAME, new BroadcastDistributionModel());
		models.put(RandomDistributionModel.MODEL_NAME, new RandomDistributionModel());
		models.put(StaticDistributionModel.MODEL_NAME, new StaticDistributionModel());
		models.put(RangeDistributionModel.MODEL_NAME, new RangeDistributionModel());
		cats = new HashMap<SchemaCacheKey<?>,CatalogEntity>();
		// create a couple of fake databases for info schema
		dbs.add(new UserDatabase("INFORMATION_SCHEMA",null));
		dbs.add(new UserDatabase("mysql",null));
		tempTableKern = ttkern;
		tempTableCounter = 0;
		try {
			defaultPolicy = CatalogHelper.generatePolicyConfig(5, OnPremiseSiteProvider.DEFAULT_NAME);
		} catch (PEException pe) {
			throw new SchemaException(Pass.FIRST, "Unable to build transient default policy");
		}
	}

	public static TwoDimensionalMap<VariableScopeKind,String,String> buildDefaultVariables() {
		TwoDimensionalMap<VariableScopeKind,String,String> out = new TwoDimensionalMap<VariableScopeKind,String,String>();
        VariableConfig<SessionVariableHandler>  config = Singletons.require(HostService.class).getSessionConfigTemplate();
		for(VariableInfo<SessionVariableHandler> vi : config.getInfoValues()) {
			String varname = vi.getName();
			String defval = vi.getDefaultValue();
			out.put(VariableScopeKind.SESSION, varname, defval);
		}
		// we turn on templates optional for all transient tests, including the analyzer
		out.put(VariableScopeKind.DVE, SchemaVariableConstants.TEMPLATE_MODE_NAME, TemplateMode.OPTIONAL.toString());
		return out;
	}
	
	public SchemaContext getPersistenceContext() {
		return tpc;
	}
	
	@Override
	public void setContext(SchemaContext cntxt) {
		tpc = cntxt;
	}
	
	public SchemaContext parse(String[] in) throws Exception {
		return parse(in,false);
	}
	
	// parse and apply.  all statements execute in the context of the project.
	public SchemaContext parse(String[] in, boolean analyzer) throws Exception {
		// we turn on ignore missing user for the analyzer.  it has no effect for the tests.
		ParserOptions opts = ParserOptions.TEST.setResolve().setIgnoreMissingUser();
		for(String sql : in) {
			SchemaContext pc = getPersistenceContext();
			pc.refresh(true);
			try {
				List<Statement> stmts = InvokeParser.parse(InvokeParser.buildInputState(sql, tpc), opts, pc).getStatements();
				for(Statement s : stmts) {
					dispatch(s);
				}
			} catch (Exception e) {
				if (analyzer)
					throw new PEException("TEE unable to parse '" + sql + "': " + e.getMessage(),e);
				throw e;
			}
		}
		SchemaContext pc = getPersistenceContext();
		pc.refresh(true);
		return pc;
	} 
	
	public void dispatch(Statement s) throws Exception {
		if (s instanceof DDLStatement) {
			dispatchCreateStatement(s);
		} else if (s instanceof SessionStatement) {
			dispatchSessionStatement(s);
		} else if (s instanceof DMLStatement) {
			// just plan these - we're primarily getting at any catalog related side effects
			Statement.getExecutionPlan(tpc,s);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public void dispatchCreateStatement(Statement s) throws Exception {
		if (s instanceof PECreateStatement) {
			executeCreateStatement((PECreateStatement<?,?>)s);
		} else if (s instanceof PEDropStatement) {
			executeDropStatement((PEDropStatement)s);
		} else {
			throw new IllegalArgumentException("Unknown create statement: " + s.getClass().getName());
		}
	}

	public void dispatchSessionStatement(Statement s) {
		if (s instanceof UseStatement) {
			UseStatement us = (UseStatement)s;
			if (us instanceof UseDatabaseStatement) {
				UseDatabaseStatement uds = (UseDatabaseStatement) us;
				tpc.setDefaultDatabase(uds.getDatabase(tpc));
				currentDatabase = uds.getDatabase(tpc);
				setCurrentTenant(null);
			}
			else if (us.getTarget() instanceof PEProject)
				tpc.setDefaultProject((PEProject)us.getTarget());
			else if (us.getTarget() instanceof PETenant) {
				PETenant ten = (PETenant) us.getTarget();
				tpc.setDefaultDatabase(ten.getDatabase(tpc));
				currentDatabase = ten.getDatabase(tpc);
				setCurrentTenant(ten);
			} else
				throw new IllegalArgumentException("Invalid use target for transient execution engine: " + us.getTarget().getClass().getName());
		} else if (s instanceof UseContainerStatement) {
			UseContainerStatement ucs = (UseContainerStatement) s;
			PEContainerTenant pect = ucs.getTenant();
			setCurrentTenant(pect);
		} else if (s instanceof SessionSetVariableStatement) {
			SessionSetVariableStatement ssvs = (SessionSetVariableStatement) s;
			// see what the var is and update our settings
			dispatchSetSessionVar(ssvs);
		} else {
			throw new IllegalArgumentException("Invalid session statement for transient execution engine: " + s.getClass().getName());
		}
	}
	
	@SuppressWarnings("rawtypes")
	public void executeCreateStatement(PECreateStatement pecs) throws Exception {
		if (pecs.getCreated() instanceof PEDatabase) {
			PECreateDatabaseStatement pecds = (PECreateDatabaseStatement) pecs;
			List<Statement> extras = pecds.getPrereqs(tpc);
			if (extras != null) {
				for(Statement s : extras) {
					dispatch(s);
				}
			}
		}
		boolean tenantCreate = pecs instanceof PECreateTenantTableStatement;
		boolean tableCreate = pecs instanceof PECreateTableStatement;
		boolean viewCreate = pecs instanceof PECreateViewStatement;
		Set<PEForeignKey> postPass = null;
		if (tableCreate) {
			pecs.normalize(tpc);
			PECreateTableStatement pect = (PECreateTableStatement) pecs;
			postPass = pect.getModifiedKeys();
		}
		Persistable p = null;
		if (viewCreate)
			p = ((PECreateViewStatement)pecs).getViewTable();
		else
			p = pecs.getCreated();
		tpc.beginSaveContext();
		PersistContext sc = null;
		try {
			p.persistTree(tpc,true);
			sc = tpc.getSaveContext();
		} finally {
			tpc.endSaveContext();
		}
		for(Map.Entry<Persistable<?,?>, CatalogEntity> nm : sc.getAssociation().entrySet()) {
			Persistable<?,?> t = nm.getKey();
			CatalogEntity cat = nm.getValue();
			if (cat instanceof UserColumn || cat instanceof DistributionModel || (cat instanceof UserTable && !tenantCreate)
					|| cat instanceof Key || cat instanceof KeyColumn)
				continue;
			if (tenantCreate && (cat instanceof UserTable)) {
				// we dynamically create the table scope via reloading callback - not available via static computation.
				// fake that out now.
				PECreateTenantTableStatement stmt = (PECreateTenantTableStatement) pecs;
				PETenant onTenant = stmt.getTenant();
				Name logicalName = (stmt.getLogicalName() == null ? stmt.getCreated().getName() : stmt.getLogicalName());
				t = onTenant.setVisible(tpc, (PETable)t, logicalName, null, null);
				cat = new TableVisibility(findTenant(PEConstants.LANDLORD_TENANT),(UserTable)cat,
						(stmt.getLogicalName() == null ? null : stmt.getLogicalName().getUnquotedName().get()),null);
			}
			SchemaCacheKey<?> sck = t.getCacheKey();
			if (sck == null) throw new PEException("Missing cache key impl for " + t.getClass().getName() + " cat entity " + cat);
			cats.put(sck,cat);
			if (cat instanceof UserDatabase) {
				UserDatabase udb = (UserDatabase) cat;
				safeAdd(dbs,udb);
				if (udb.getMultitenantMode().isMT()) {
					Tenant exists = findTenant(PEConstants.LANDLORD_TENANT);
					if (exists == null) {
						exists = new Tenant(udb, PEConstants.LANDLORD_TENANT, PEConstants.LANDLORD_TENANT);
						setID(exists,42);
						safeAdd(tenants,exists);
					}
				}
			}
			else if (cat instanceof PersistentGroup)
				safeAdd(groups,(PersistentGroup)cat);
			else if (cat instanceof Project)
				safeAdd(projects,(Project)cat);
			else if (cat instanceof PersistentSite)
				safeAdd(sites,(PersistentSite)cat);
			else if (cat instanceof DistributionRange)
				safeAdd(rds,(DistributionRange)cat);
			else if (cat instanceof RangeTableRelationship) {
				RangeTableRelationship rtr = (RangeTableRelationship) cat;
				rangesForTables.put(rtr.getTable(),rtr);
			} else if (cat instanceof TableVisibility) {
				TableVisibility tv = (TableVisibility) cat;
				scopes.put(PEConstants.LANDLORD_TENANT, tv);
			} else if (cat instanceof Container) {
				safeAdd(containers, (Container)cat);
			} else if (cat instanceof PersistentTemplate) {
				safeAdd(templates, (PersistentTemplate)cat);
			} else if (cat instanceof UserView) {
				safeAdd(views, (UserView)cat);
			} else if (cat instanceof User) {
				safeAdd(users, (User)cat);
			}
			
			else
				throw new PEException("Unknown item: " + cat);
		}
		if (postPass != null) {
			tpc.beginSaveContext();
			try {
				for(PEForeignKey pefk : postPass) {
					// flush the changes out
					pefk.persistTree(tpc,true);
				}
			} finally {
				tpc.endSaveContext();
			}

		}
	}
	
	private <T> void safeAdd(List<T> into, T obj) {
		if (!into.contains(obj))
			into.add(obj);
	}
	
	
	public void executeDropStatement(PEDropStatement<?, ?> peds) throws Exception {
	}
	
	@Override
	public Project findProject(String name) {
		for(Project p : projects)
			if (p.getName().equals(name))
				return p;
		return null;
	}

	@Override
	public Project createProject(String name) {
		return new Project(name);
	}

	@Override
	public PersistentGroup findPersistentGroup(String name) {
		for(PersistentGroup pg : groups)
			if (pg.getName().equals(name))
				return pg;
		return null;
	}

	@Override
	public PersistentGroup createPersistentGroup(String name) {
		return new PersistentGroup(name);
	}

	@Override
	public PersistentSite createPersistentSite(String name, String haType, 
			SiteInstance master, SiteInstance[] replicants) throws PEException {
		PersistentSite site = new PersistentSite(name, haType);
		site.setMasterInstance(master);
		site.addAll(replicants);
		return site;
	}

	@Override
	public PersistentSite createPersistentSite(String name, String url, String user, String password) throws PEException {
		return new PersistentSite(name, new SiteInstance(name, url, user, password));
	}

	@Override
	public PersistentSite findPersistentSite(String name) {
		for(PersistentSite ps : sites)
			if (ps.getName().equals(name))
				return ps;
		return null;
	}

	@Override
	public SiteInstance findSiteInstance(String name) {
		return null;
	}

	@Override
	public SiteInstance createSiteInstance(String name, String url, String user, String password, boolean master, String status) {
		return null;
	}
	
	@Override
	public void persistToCatalog(Object o) {
	}

	@Override
	public Map<String, DistributionModel> getDistributionModelMap() {
		return models;
	}

	@Override
	public UserDatabase findUserDatabase(String n) {
		for(UserDatabase ud : dbs)
			if (ud.getName().equals(n))
				return ud;
		return null;
	}

	@Override
	public UserTable findUserTable(Name name, int dbid, String dbName) {
		UserDatabase udb = null;
		for(UserDatabase u : dbs) {
			if (u.getName().equals(dbName)) {
				udb = u;
				break;
			}
		}
		if (udb == null) return null;
		try {
			return udb.getTableByName(name.get(), false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public TableVisibility findTableVisibility(String tenantName, int tenantID,
			String tableName) {
		Collection<TableVisibility> tenscopes = scopes.get(tenantName);
		if (tenscopes == null) return null;
		for(TableVisibility tv : tenscopes) {
			if (tv.getTable().getName().equals(tableName) || tableName.equals(tv.getLocalName()))
				return tv;
		}
		return null;
	}
		
	@Override
	public DistributionRange findDistributionRange(String name, String groupName) {
		for(DistributionRange dr : rds) 
			if (dr.getName().equals(name) && dr.getStorageGroup().getName().equals(groupName))
				return dr;
		return null;
	}

	@Override
	public List<DistributionRange> findDistributionRange(final String name) {
		return Functional.select(rds, new UnaryPredicate<DistributionRange>() {

			@Override
			public boolean test(DistributionRange object) {
				return object.getName().equals(name);
			}
			
		});
	}
	
	@Override
	public RangeTableRelationship findRangeTableRelationship(UserTable forTable) {
		return rangesForTables.get(forTable);
	}

	@Override
	public boolean isPersistent() {
		// the persistent schema holds less info than the transient schema - we do some extra tests with the 
		// transient schema in the meantime.
		return false;
	}

	@Override
	public void addAutoIncrement(SchemaContext sc, PETable onTable, Long offset) {
		autoincs.put(onTable.getPersistent(tpc), (offset == null ? new Long(1): offset));
	}

	@Override
	public long getNextIncrementValue(SchemaContext sc, PETable onTable) {
		UserTable ut = onTable.getPersistent(tpc);
		Long v = autoincs.get(ut);
		Long ret = new Long(v.longValue() + 1);
		autoincs.put(ut, ret);
		return v.longValue();
	}

	@Override
	public long getNextIncrementValueChunk(SchemaContext sc, PETable onTable, long blockSize) {
		UserTable ut = onTable.getPersistent(tpc);
		Long v = autoincs.get(ut);
		Long mark = new Long(v.longValue() + blockSize);
		autoincs.put(ut, mark);
		return v.longValue();
	}

	@Override
	public String getTempTableName() {
		tempTableCounter++;
		return tempTableKern + tempTableCounter;
	}

	@Override
	public MappingSolution mapKey(SchemaContext sc, IKeyValue dk, Model model, DistKeyOpType op, PEStorageGroup onGroup) throws PEException {
		// we don't do anything different by op type, but the persistent version does.
		if (Model.RANDOM == model )
			return RandomDistributionModel.SINGLETON.mapKeyForQuery(null, onGroup.getPersistent(sc), dk, op);
		if (Model.BROADCAST == model)
			return BroadcastDistributionModel.SINGLETON.mapKeyForQuery(null, onGroup.getPersistent(sc), dk, op);
		if (Model.STATIC == model )
			return StaticDistributionModel.SINGLETON.mapKeyForQuery(null, onGroup.getPersistent(sc), dk, op);
		if (Model.RANGE == model) 
			// we use static dist model in place of range for the trans exec engine since range
			// does a catalog lookup and there is no catalog
			return StaticDistributionModel.SINGLETON.mapKeyForQuery(null, onGroup.getPersistent(sc), dk, op);

		throw new PEException("Unknown dist model kind: " + model.getPersistentName());
	}

	private static final String SINGLE_SITE_GROUP_QUERY = 
			"SELECT a FROM PersistentSite AS a ORDER BY a.name ASC";
	
	@Override
	public List<CatalogEntity> query(String query, Map<String, Object> params) {
		if (SINGLE_SITE_GROUP_QUERY.equals(query) && params.isEmpty())
			return Functional.apply(sites, new UnaryFunction<CatalogEntity,PersistentSite>() {

				@Override
				public CatalogEntity evaluate(PersistentSite object) {
					return object;
				}
				
			});
		throw new SchemaException(Pass.SECOND, "No support for catalog query in transient execution engine");
	}

	@Override
	public Project getDefaultProject() {
		throw new SchemaException(Pass.SECOND, "TransientExecutionEngine does not use default project");
	}

	@Override
	public Object findByKey(Class<? extends CatalogEntity> c, int id) {
		return null;
	}

	@Override
	public UserTable createTempTable(UserDatabase udb, String name,
			DistributionModel dist) {
		try {
			return UserTable.newTempTable(udb, null, name, dist);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public List<User> findUsers(String name, String accessSpec) {
		List<User> out = new ArrayList<User>();
		for(User u : users) {
			if (u.getName().equals(name) && u.getAccessSpec().equals(accessSpec))
				out.add(u);
		}
		return out;
	}

	@Override
	public List<UserDatabase> findAllUserDatabases() {
		return dbs;
	}

	@Override
	public Tenant findTenant(String extID) {
		for(Tenant t : tenants) 
			if (t.getExternalTenantId().equals(extID))
				return t;
		return null;
	}

	@Override
	public List<PersistentGroup> findAllGroups() {
		return groups;
	}

	@Override
	public PersistentGroup findBalancedPersistentGroup(String prefix) {
		return groups.get(0);
	}

	@SuppressWarnings("unchecked")
	@Override
	public SchemaEdge<IPETenant> getCurrentTenant() {
		if (currentTenant == null)
			return StructuralUtils.buildEdge(tpc,null,true);
		else
			return StructuralUtils.buildEdge(tpc,currentTenant, true);
	}

	@Override
	public boolean allowTenantColumnDecls() {
		return true;
	}

	@Override
	public void setSchemaContext(SchemaContext sc) {
		// ignore, we are our own schema context
	}

	@Override
	public TableVisibility findVisibilityRecord(int tabid, int tenid) {
		return null;
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, TableKey tk) {
		return null;
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, PETable tk) {
		return null;
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, TableScope tk) {
		return null;
	}

	
	@Override
	public Provider findProvider(String name) {
		return null;
	}

	@Override
	public List<Provider> findAllProviders() {
		return null;
	}
	
	@Override
	public DynamicPolicy getDynamicGroupPolicy() {
		return TransientExecutionEngine.transformTestPolicy;
	}

	@Override
	public DynamicPolicy findPolicy(String name) {
		if (OnPremiseSiteProvider.DEFAULT_POLICY_NAME.equals(name)) {
			return defaultPolicy;
		}
		return null;
	}

	@Override
	public List<CatalogEntity> nativeQuery(String query,
			Map<String, Object> params, Class<?> targetClass) {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List nativeQuery(String query, Map<String, Object> params) {
		return Collections.emptyList();
	}

	@Override
	public String getVariableValue(VariableAccessor va) throws PEException {
		return variables.get(va.getScopeKind(),va.getVariableName());
	}

	@Override
	public List<List<String>> getVariables(VariableScope vs) throws PEException {
		throw new PEException("No support for getVariables in TransientExecutionEngine");
	}

	public TwoDimensionalMap<VariableScopeKind, String, String> getVariables() {
		return variables;
	}

	private void dispatchSetSessionVar(SessionSetVariableStatement stmt) {
		for(SetExpression se : stmt.getSetExpressions()) {
			if (se.getKind() == SetExpression.Kind.TRANSACTION_ISOLATION)
				throw new SchemaException(Pass.PLANNER, "No support for setting txn isolation in trans exec engine");
			SetVariableExpression sve = (SetVariableExpression) se;
			VariableScope vs = sve.getVariable().getScope();
			String name = VariableConfig.canonicalise(sve.getVariable().getVariableName().getUnquotedName().get());
			Object value = sve.getVariableValue(tpc);
			variables.put(vs.getScopeKind(), name, value.toString());
		}
	}
	
	@Override
	public List<PersistentSite> findAllSites() {
		return sites;
	}

	@Override
	public void setCurrentTenant(IPETenant ten) {
		currentTenant = ten;
	}

	@Override
	public List<UserDatabase> findAllMTDatabases() {
		ArrayList<UserDatabase> out = new ArrayList<UserDatabase>();
		for(UserDatabase udb : dbs)
			if (udb.getMultitenantMode().isMT())
				out.add(udb);
		return out;
	}

	@SuppressWarnings("unchecked")
	@Override
	public SchemaEdge<PEUser> getUser() {
		return StructuralUtils.buildEdge(tpc,currentUser,false);
	}

	@SuppressWarnings("unchecked")
	@Override
	public SchemaEdge<Database<?>> getCurrentDatabase() {
		return StructuralUtils.buildEdge(tpc,currentDatabase, true);
	}
	
	@Override
	public List<String> findTenantTableNames(Tenant t) {
		throw new IllegalStateException("No support for findTenantTableNames in transient execution engine");
	}

	@Override
	public long readNextIncrementValue(SchemaContext sc, TableKey tk) {
		return -1;
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, TableKey tk, long value) {
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, PETable tab, long value) {
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, TableScope ts, long value) {
	}

	
	@Override
	public String getName() {
		return "TransientExecutionEngine";
	}

	@Override
	public void refresh(CatalogEntity cat,boolean pessimistic) {
		// no backing so by default no refresh
	}

	@Override
	public void acquireLock(LockSpecification ls, LockType type) {
	}

	@Override
	public boolean isInTxn() {
		return false;
	}

	@Override
	public CatalogDAO getDAO() {
		throw new IllegalStateException("No support for catalogDAO in transient execution engine");
	}

	@Override
	public List<TableVisibility> findTenantsOf(UserTable tab) {
		throw new IllegalStateException("No support for findTenantsOf in transient execution engine");
	}

	@Override
	public List<UserTable> findMatchingTables(UserDatabase udb,
			String logicalName, String definition) {
		throw new IllegalStateException("No support for findMatchingTables in transient execution engine");
	}

	@Override
	public ExternalService findExternalService(String name) {
		return null;
	}

	@Override
	public ExternalService createExternalService(String name, String plugin,
			String connectUser, boolean usesDataStore, String config) {
		return null;
	}

	@Override
	public Priviledge findPrivilege(SchemaContext sc, int userid, String name) {
		return null;
	}

	@Override
	public boolean findGlobalPrivilege(SchemaContext sc, PEUser user) {
		return false;
	}
	
	public static class TransientDynamicGroup extends PEDynamicGroup {

		private int size;

		public TransientDynamicGroup(GroupScale scale, int size) {
			super(scale);
			this.size = size;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof PEDynamicGroup)) return false;
			PEDynamicGroup pedg = (PEDynamicGroup) other;
			return getScale() == pedg.getScale();
		}

		public String getProviderName() {
			return "NoProvider";
		}

		public String getClassName() {
			return "NoClass";
		}

		@Override
		public String toString() {
			return "TransientDynamicGroup(" + getProviderName() + "/" + getClassName() + "/" + getScale() + "/" + getCount() +")";
		}

		public int getCount() {
			return size;
		}

		public String getCodeName() {
			String suffix = null;
			if (getScale() == GroupScale.AGGREGATE)
				suffix = "AGGREGATION";
			else
				suffix = getScale().toString();
			return "TransientExecutionEngine." + suffix;
		}

	}

	public static final TransientDynamicGroup LARGE = new TransientDynamicGroup(GroupScale.LARGE,8);
	public static final TransientDynamicGroup MEDIUM = new TransientDynamicGroup(GroupScale.MEDIUM,4);
	public static final TransientDynamicGroup SMALL = new TransientDynamicGroup(GroupScale.SMALL,2);
	public static final TransientDynamicGroup AGGREGATION = new TransientDynamicGroup(GroupScale.AGGREGATE,1);

	// all of the transform tests for now use the same object for the policy
	public static final DynamicPolicy transformTestPolicy = new DynamicPolicy(
			"TransformTestPolicy", true,
			AGGREGATION.getProviderName(), AGGREGATION.getClassName(), AGGREGATION.getCount(),
			SMALL.getProviderName(), SMALL.getClassName(), SMALL.getCount(),
			MEDIUM.getProviderName(), MEDIUM.getClassName(), MEDIUM.getCount(),
			LARGE.getProviderName(), LARGE.getClassName(), LARGE.getCount());

	@Override
	public <T extends CatalogEntity> T findByKey(PersistentCacheKey pck) {
		return null;
	}

	@Override
	public EntityCacheKey buildCacheKey(CatalogEntity ce) {
		return new TransientCacheKey(ce);
	}

	private static class TransientCacheKey implements EntityCacheKey {
		
		private final Class<?> entityClass;
		private final int givenHashcode;
		
		public TransientCacheKey(CatalogEntity cat) {
			entityClass = cat.getClass();
			givenHashcode = cat.hashCode();
		}

		@Override
		public String toString() {
			return "TransientCacheKey{" + entityClass.getSimpleName() + "@" + givenHashcode + "}";
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((entityClass == null) ? 0 : entityClass.hashCode());
			result = prime * result + givenHashcode;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TransientCacheKey other = (TransientCacheKey) obj;
			if (entityClass == null) {
				if (other.entityClass != null)
					return false;
			} else if (!entityClass.equals(other.entityClass))
				return false;
			if (givenHashcode != other.givenHashcode)
				return false;
			return true;
		}

	}
	
	public static void setID(Object o, int val) {
		Class<?> c = o.getClass();
		try {
			Field f = c.getDeclaredField("id");
			f.setAccessible(true);
			f.set(o, val);
		} catch (Throwable t) {
			throw new RuntimeException("unable to force id on class " + c.getSimpleName(),t);
		}
	}

	@Override
	public long getNextIncrementValueChunk(SchemaContext sc, TableScope ts,
			long blockSize) {
		return 0;
	}

	@Override
	public Container findContainer(String name) {
		for(Container container : containers)
			if (container.getName().equals(name))
				return container;
		return null;
	}

	@Override
	public List<UserTable> findContainerMembers(String containerName)
			throws PEException {
		return null;
	}
	
	@Override
	public ContainerTenant findContainerTenant(String containerName,
			String discriminant) {
		for(ContainerTenant ct : containerTenants) {
			if (containerName == null && discriminant == null) {
				if (ct.isGlobalTenant())
					return ct;
			} else if (ct.isGlobalTenant()) {
				continue;
			} else if (ct.getContainer().getName().equals(containerName) && ct.getDiscriminant().equals(discriminant))
				return ct;
		}
		return null;
	}

	@Override
	public ConnectionContext copy() {
		return this;
	}

	@Override
	public CatalogContext copy(ConnectionContext cc) {
		return this;
	}
	
	@Override
	public void saveContainerTenant(Container cont, String disc) {
		ContainerTenant ct = new ContainerTenant(cont,disc);
		setID(ct,containerTenants.size() + 1);
		containerTenants.add(ct);
	}

	@Override
	public List<UserTable> findTablesWithUnresolvedFKsTargeting(
			String schemaName, String tableName) {
		UserDatabase udb = findUserDatabase(schemaName);
		if (udb == null)
			throw new SchemaException(Pass.PLANNER, "No such database: " + schemaName);
		ListSet<UserTable> matching = new ListSet<UserTable>();
		for(UserTable ut : udb.getUserTables()) {
			for(Key k : ut.getKeys()) {
				if (!k.isForeignKey()) continue;
				if (k.getReferencedTable() == null && k.getReferencedTableName().equals(tableName)) {
					matching.add(ut);
					break;
				}
			}
		}
		return matching;
	}

	@Override
	public List<UserTable> findTablesWithFKSReferencing(int tabID) {
		return null;
	}

	@Override
	public void startTxn() {
	}

	@Override
	public void commitTxn() {
	}

	@Override
	public void rollbackTxn() {
	}

	@Override
	public HollowTableVisibility findHollowTableVisibility(String tenantName, int tenantID, String tableName) {
		throw new IllegalStateException("no hollow support in trans. exec. engine");
	}

	@Override
	public boolean hasFilter() {
		return false;
	}
	
	@Override
	public boolean isFilteredTable(Name table) {
		return false;
	}

	@Override
	public boolean originatedFromReplicationSlave() {
		return false;
	}
	
	@Override
	public String getCacheName() {
		return null;
	}

	@Override
	public int getConnectionId() {
		return 67;
	}

	@Override
	public Key findForeignKey(UserDatabase db, Integer tenantID, String name, String constraintName) throws PEException {
		return null;
	}

	@Override
	public ConnectionMessageManager getMessageManager() {
		return messages;
	}

	@Override
	public long getLastInsertedId() {
		return -1;
	}

	@Override
	public PersistentTemplate findTemplate(String name) {
		for(PersistentTemplate pt : templates)
			if (pt.getName().equals(name))
				return pt;
		return null;
	}

	@Override
	public List<PersistentTemplate> findMatchTemplates() {
		TreeMap<String, PersistentTemplate> byName = new TreeMap<String,PersistentTemplate>();
		for(PersistentTemplate pt : templates)
			if (pt.getMatch() != null)
				byName.put(pt.getName(), pt);
		return Functional.toList(byName.values());
	}

	@Override
	public RawPlan findRawPlan(String name) {
		return null;
	}

	@Override
	public List<RawPlan> findAllEnabledRawPlans() {
		return Collections.emptyList();
	}

	@Override
	public List<TableVisibility> findScopesWithUnresolvedFKsTargeting(
			String schemaName, String tableName, int tenantID) {
		return null;
	}

	@Override
	public List<TableVisibility> findScopesWithFKsReferencing(int tableID,
			int tenantID) {
		return null;
	}

	@Override
	public List<TableVisibility> findScopesForFKTargets(int tableID,
			int tenantID) {
		return null;
	}

	@Override
	public UserView findView(String name, String dbn) {
		for(UserView uv : views) {
			if (uv.getTable().getName().equals(name) && uv.getTable().getDatabase().getName().equals(dbn))
				return uv;
		}
		return null;
	}
	
	public void setCurrentDatabase(Database<?> db) {
		this.currentDatabase = db;
	}

	@Override
	public List<UserTable> findTablesOnGroup(String groupName) {
		return null;
	}

	@Override
	public List<User> findAllUsers() {
		return null;
	}

	@Override
	public boolean isInXATxn() {
		return false;
	}

}
