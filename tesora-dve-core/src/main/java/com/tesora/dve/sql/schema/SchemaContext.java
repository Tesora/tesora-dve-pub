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


import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.antlr.runtime.TokenStream;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.IDynamicPolicy;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentTemplate;
import com.tesora.dve.common.catalog.Priviledge;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.common.catalog.RawPlan;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.infomessage.ConnectionMessageManager;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheType;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.cache.SchemaSource;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.schema.mt.TableScope.ScopeCacheKey;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.variable.VariableAccessor;
import com.tesora.dve.variables.AbstractVariableAccessor;
import com.tesora.dve.variables.VariableStoreSource;
import com.tesora.dve.variables.Variables;
import com.tesora.dve.worker.agent.Agent;

public class SchemaContext {

	public static ThreadLocal<SchemaContext> threadContext = new ThreadLocal<SchemaContext>();
	
	public static void clearThreadContext() {
		threadContext.set(null);
	}
	
	public static void setThreadContext(SchemaContext sc) {
		threadContext.set(sc);
	}
	
	private SchemaSource schemaSource;
	private final IdentityHashMap<Persistable<?,?>, Serializable> backing;
	private final Map<SchemaCacheKey<?>, Persistable<?,?>> loading = new HashMap<SchemaCacheKey<?>, Persistable<?,?>>();
	private final Set<SchemaCacheKey<?>> cacheLoading = new HashSet<SchemaCacheKey<?>>();
	private final NativeTypeCatalog types;
	private final TemporaryTableSchema temporaryTableSchema = new TemporaryTableSchema();
	private CatalogContext catalog;
	private PersistContext saveContext;
	private ParserOptions opts;
	private SchemaEdge<PEProject> defaultProject = null;
	private ConnectionContext connection = null;
	private boolean mutableSource;
	private Boolean mutableSourceOverride;

	private SchemaPolicyContext perms;

	private BehaviorConfiguration configuration;
		
	private long tableCounter;
	private long objectCounter = 0;
	
	private ValueManager valueManager;
	private ConnectionValues values;
	private boolean usedCachedPlan = false;
	
	private final TransientSessionState tss;
	
	private TokenStream tokens;
	
	private String description = null;
	
	private static ConnectionContext buildConnectionContext(SSConnection conn) {
		if (conn == null) return new NullConnectionContext(null);
		return new SSConnectionContext(conn);
	}
	
	public static SchemaContext createContext(SSConnection conn) {
		if (conn == null)
			throw new IllegalArgumentException("Wrong constructor for null connection");
		ConnectionContext cc = buildConnectionContext(conn);
		return new SchemaContext(new DAOContext(cc),cc);
	}
	
	public static SchemaContext createContext(CatalogDAO dao) {
		ConnectionContext cc = new NullConnectionContext(dao);
		return new SchemaContext(new DAOContext(cc),cc);
	}
	
	public static SchemaContext createContext(SchemaContext other) {
		return createContext(other.getCatalog(), other.getConnection());
	}
	
	public static SchemaContext createContext(CatalogContext cntxt, ConnectionContext conn) {
		return new SchemaContext(cntxt, conn);
	}

	public static Database<?> loadDB(SchemaContext pc, UserDatabase db) {
		if (db == null) return null;
        Database<?> peds = Singletons.require(HostService.class).getInformationSchema().buildPEDatabase(pc, db);
		if (peds == null)
			peds = PEDatabase.load(db, pc);
		return peds;
	}
	
	// used in alter support
	public static SchemaContext createContext(SSConnection conn, SchemaContext basedOn) {
		SchemaContext out = SchemaContext.createContext(conn);
		out.setOptions(basedOn.getOptions());
		out.valueManager = basedOn.valueManager;
		out.setValues(basedOn.values);
		return out;
	}
	
	// used in container tenant support
	public static SchemaContext makeMutableIndependentContext(SchemaContext basedOn) {
		ConnectionContext cc = basedOn.getConnection().copy();
		SchemaContext out = new SchemaContext(basedOn.getCatalog().copy(cc), cc);
		out.forceMutableSource();
		return out;
	}
	
	// used in raw plan support
	public static SchemaContext makeImmutableIndependentContext(SchemaContext basedOn) {
		ConnectionContext cc = basedOn.getConnection().copy();
		SchemaContext out = new SchemaContext(basedOn.getCatalog().copy(cc), basedOn.getConnection().copy());
		return out;
	}
	
	@SuppressWarnings("unchecked")
	private SchemaContext(CatalogContext cat, ConnectionContext conn) {
		// always start out unmutable
		mutableSource = false;
		connection = conn;
		catalog = cat;
		schemaSource = null;
		setSource(buildSource());
		defaultProject = StructuralUtils.buildEdge(this, null, false); 
		cat.setContext(this);
		conn.setSchemaContext(this);
        types = Singletons.require(HostService.class).getDBNative().getTypeCatalog();
		backing = new IdentityHashMap<Persistable<?,?>, Serializable>();
		saveContext = null;
		opts = null;
		tableCounter = 0;
		valueManager = new ValueManager();
		setValues(null);
		perms = null;
		tss = new TransientSessionState();
	}

	// a partial refresh - clear all expensive fields that won't be accessed later
	public void cleanupPostPlanning() {
		tokens = null;
	}
	
	public SchemaSource getSource() {
		return schemaSource;
	}
	
	public void setSource(SchemaSource ss) {
		schemaSource = ss;
	}
	
	private SchemaSource buildSource() {
		return (mutableSource ? SchemaSourceFactory.getMutableSource() : SchemaSourceFactory.getSource(connection, schemaSource));
	}
	
	public void forceMutableSource() {
		if (schemaSource.getType() != CacheType.MUTABLE) {
			mutableSource = true;
			setSource(buildSource());
		}
	}
	
	public void forceImmutableSource() {
		if (schemaSource.getType() == CacheType.MUTABLE) {
			mutableSource = false;
			setSource(buildSource());
		}
	}
	
	public boolean isMutableSource() {
		if (mutableSourceOverride != null) return mutableSourceOverride.booleanValue();
		return schemaSource.getType() == CacheType.MUTABLE;
	}
	
	public void setMutableSourceOverride(Boolean b) {
		mutableSourceOverride = b;
	}
	
	public Boolean getMutableSourceOverride() {
		return mutableSourceOverride;
	}
	
	public <T> SchemaEdge<T> buildEdgeFromKey(SchemaCacheKey<T> sck, boolean persistent) {
		if (!persistent) throw new SchemaException(Pass.SECOND, "Invalid use of buildEdge - using key based build edge on a transient edge");
		return getSource().buildEdgeFromKey(sck);
	}
	
	public void refresh() {
		refresh(true);
	}
	
	public void refresh(boolean clearValues) {
		// refreshing always forces it unmutable
		mutableSource = false; 
		setSource(buildSource());
		loading.clear();
		cacheLoading.clear();
		if (catalog.isPersistent()) {
			backing.clear();
		}
		tableCounter = 0;
		if (clearValues) {
			valueManager = new ValueManager();
			setValues(null);
		}
		perms = null;
		usedCachedPlan = false;
	}
	
	public long getNextTable() {
		return (++tableCounter);
	}
	
	public long getNextObjectID() {
		return (++objectCounter);
	}
	
	public ParserOptions getOptions() {
		return opts;
	}
	
	public String describeContext() {
		if (description == null) {
			StringBuilder buf = new StringBuilder();
			if (connection != null) {
				buf.append(connection.getName());
				SchemaEdge<?> edge = connection.getCurrentDatabase();
				Database<?> cdb = (edge == null ? null : connection.getCurrentDatabase().get(this));
				IPETenant ct = getCurrentTenant().get(this); 
				if (cdb != null)
					buf.append("/").append(cdb.getName());
				if (ct != null)
					buf.append("/").append(ct.getUniqueIdentifier());
			}
			description = buf.toString();
		}
		return description;
	}
	
	public void setOptions(ParserOptions po) {
		opts = po;
	}

	public void setTokenStream(TokenStream tns) {
		if (tns == null)
			throw new SchemaException(Pass.FIRST, "Parser missing tokens");
		tokens = tns;
	}
	
	public TokenStream getTokens() {
		return tokens;
	}
	
	public void setParameters(List<Object> params) {
		valueManager = new ValueManager(this,params);
	}
	
	public ValueManager getValueManager() {
		return valueManager;
	}
	
	public void setValueManager(ValueManager vm) {
		valueManager = vm;
	}
	
	public ConnectionValues _getValues() {
		return values;
	}
	
	public void setValues(ConnectionValues cv) {
		values = cv;
	}
	
	public void setUsedCachedPlan() {
		usedCachedPlan = true;
	}
	
	public boolean getUsedCachedPlan() {
		return usedCachedPlan;
	}


	public TransientSessionState getIntraStmtState() {
		return tss;
	}
	
	public BehaviorConfiguration getBehaviorConfiguration() {
		return configuration;
	}
	
	public Database<?> getCurrentDatabase(boolean mustExist, boolean domtchecks) {
		if ((connection.getCurrentDatabase() == null || connection.getCurrentDatabase().get(this) == null) && mustExist) 
			throw new SchemaException(Pass.SECOND,"Current database not set");
		if (connection.getCurrentDatabase() == null) return null;
		if (!domtchecks) 
			return connection.getCurrentDatabase().get(this);
		return getPolicyContext().getCurrentDatabase(connection.getCurrentDatabase().get(this), mustExist);
	}
	
	public Database<?> getCurrentDatabase(boolean mustExist) {
		return getCurrentDatabase(mustExist,true);
	}
	
	public PEDatabase getCurrentPEDatabase(boolean mustExist) {
		return (PEDatabase)getCurrentDatabase(mustExist);
	}
	
	public boolean hasCurrentDatabase() {
		return connection.getCurrentDatabase() != null && connection.getCurrentDatabase().get(this) != null;
	}
	
	public Database<?> getCurrentDatabase() {
		return getCurrentDatabase(true);
	}

	public PEDatabase getCurrentPEDatabase() {
		return getCurrentPEDatabase(true);
	}
	
	public void setCurrentDatabase(Database<?> db) {
		connection.setCurrentDatabase(db);
	}
	
	public ConnectionContext getConnection() {
		return connection;
	}
	
	public SchemaEdge<PEUser> getCurrentUser() {
		return getConnection().getUser();
	}
	
	public SchemaEdge<IPETenant> getCurrentTenant() {
		return getConnection().getCurrentTenant();
	}
	
	public void invalidateCurrentTenant() {
		getConnection().setCurrentTenant(null);
	}
	
	public SchemaPolicyContext getPolicyContext() {
		if (perms == null)
			perms = SchemaPolicyContext.buildContext(this);
		return perms;
	}
	
	public void clearPolicyContext() {
		perms = null;
	}
	
	public TemporaryTableSchema getTemporaryTableSchema() {
		return temporaryTableSchema;
	}
	
	public PEPersistentGroup getPersistentGroup() {
		Database<?> db = getCurrentDatabase(false);
		if (db instanceof PEDatabase) {
			PEDatabase peds = (PEDatabase) db; 
			if (peds != null)
				return peds.getDefaultStorage(this);
		}
		return getDefaultProject().getDefaultStorageGroup();
	}
	
	public PEPersistentGroup getSessionStatementStorageGroup() throws PEException {
		PEPersistentGroup pesg = getPersistentGroup();
		if (pesg != null && !pesg.getSites(this).isEmpty()) return pesg.anySite(this);
		List<PersistentGroup> groups = catalog.findAllGroups();
		if (groups.isEmpty()) 
			return null;
		// make sure we choose a group that has at least one site in it
		ListSet<PersistentGroup> choices = new ListSet<PersistentGroup>();
		choices.addAll(groups);
		PersistentGroup choice = null;
		while(choice == null) {
			if (choices.isEmpty())
				return null;
			choice = choices.remove(Agent.getRandom(choices.size()));
			if (choice.getStorageSites().isEmpty()) 
				choice = null;
		}
		pesg = PEPersistentGroup.load(choice, this);
		return pesg.anySite(this);
	}
	
	@SuppressWarnings("unchecked")
	public PEProject getDefaultProject() {
		if (!defaultProject.has()) {
            defaultProject = StructuralUtils.buildEdge(this, schemaSource.find(this,PEProject.getProjectKey(Singletons.require(HostService.class).getDefaultProjectName())),true);
		}
		return defaultProject.get(this);
	}

	public void setDefaultDatabase(Database<?> db) {
		getConnection().setCurrentDatabase(db);
	}
	
	@SuppressWarnings("unchecked")
	public void setDefaultProject(PEProject pep) {
		defaultProject = StructuralUtils.buildEdge(this,pep,false);
	}
	
	public boolean isPersistent() {
		return catalog.isPersistent();
	}
	
	public PersistContext beginSaveContext() {
		return beginSaveContext(false);
	}

	public PersistContext beginSaveContext(boolean forceNew) {
		if (forceNew || saveContext == null)
			saveContext = new PersistContext();
		saveContext.incrementRefCount();
		return saveContext;
	}
	
	public PersistContext getSaveContext() {
		return saveContext;
	}
	
	public void endSaveContext() {
		if (saveContext == null) return;
		if (saveContext.decrementRefCount())
			saveContext = null;
	}
	
	public Persistable<?,?> getLoaded(CatalogEntity cat, SchemaCacheKey<?> sck) {
		Persistable<?,?> candidate = loading.get(sck);
		if (candidate != null) return candidate;
		return (Persistable<?,?>) schemaSource.getLoaded(sck);
	}
	
	public void startLoading(Persistable<?,?> p, CatalogEntity ce) {
		SchemaCacheKey<?> sck = p.getCacheKey();
		if (sck != null) loading.put(sck,p);
	}
	
	public void finishedLoading(Persistable<?,?> p, CatalogEntity cat) {
		SchemaCacheKey<?> sck = p.getCacheKey();
		if (sck != null)
			loading.remove(sck);
		schemaSource.setLoaded(p, (cacheLoading.contains(sck) ? null : sck));
	}
	
	public void addCacheLoading(SchemaCacheKey<?> sck) {
		cacheLoading.add(sck);
	}
	
	public void removeCacheLoading(SchemaCacheKey<?> sck) {
		cacheLoading.remove(sck);
	}
	
	public boolean isCacheLoading(SchemaCacheKey<?> sck) {
		return cacheLoading.contains(sck);
	}
	
	public void clearLoaded(SchemaCacheKey<?> sck) {
		schemaSource.setLoaded(null, sck);
	}
	
	public Serializable getBacking(Persistable<?,?> p) {
		return backing.get(p);
	}
	
	public void setBacking(Persistable<?,?> p, Serializable s) {
		backing.put(p,s);
	}
	
	public NativeTypeCatalog getTypes() {
		return types;
	}

	public CatalogContext getCatalog() {
		return catalog;
	}
	
	public void persist(Object o) {
		catalog.persistToCatalog(o);
	}
	
	public IDynamicPolicy getGroupPolicy() {
		String defName =
				Variables.DEFAULT_DYNAMIC_POLICY.getValue(getConnection().getVariableSource());
		if (defName == null) return null;
		return (IDynamicPolicy)schemaSource.find(this, PEPolicy.getPolicyKey(defName));
	}
	
	public PEProject findProject(Name n) {
		String persistentName = n.getUnqualified().get();
		Project proj = catalog.findProject(persistentName);
		if (proj == null)
			return null;
		return PEProject.load(proj, this);
	}

	public RangeDistribution findRange(Name rangeName, Name groupName) {
		return schemaSource.find(this, RangeDistribution.buildRangeKey(rangeName, groupName));
	}
	
	public PEPersistentGroup findStorageGroup(Name n) {
		return schemaSource.find(this, PEPersistentGroup.getGroupKey(n));
	}
	
	public PEStorageSite findStorageSite(Name n) {
		return schemaSource.find(this, PEStorageSite.getSiteKey(n));
	}

	public PETemplate findTemplate(Name n) {
		return schemaSource.find(this, PETemplate.getTemplateKey(n.getUnqualified()));
	}
		
	public PERawPlan findRawPlan(Name n) {
		return schemaSource.find(this, PERawPlan.getRawPlanKey(n.getUnqualified()));
	}
	
	public List<PERawPlan> findEnabledRawPlans() {
		List<RawPlan> rps = catalog.findAllEnabledRawPlans();
		final SchemaContext sc = this;
		return Functional.apply(rps, new UnaryFunction<PERawPlan,RawPlan>() {

			@Override
			public PERawPlan evaluate(RawPlan object) {
				return PERawPlan.load(object, sc);
			}
			
		});
	}
	
	public PEDatabase findSingleMTDatabase() {
		List<UserDatabase> mtdbs = catalog.findAllMTDatabases();
		if (mtdbs.isEmpty())
			return null;
		if (mtdbs.size() > 1)
			throw new SchemaException(Pass.SECOND, "More than one mt database found");
		return (PEDatabase) loadDB(this,mtdbs.get(0));
	}
	
	public List<Database<?>> findDatabases() {
		List<UserDatabase> uds = catalog.findAllUserDatabases();
		final SchemaContext sc = this;
		return Functional.apply(uds, new UnaryFunction<Database<?>, UserDatabase>() {

			@Override
			public Database<?> evaluate(UserDatabase object) {
				return loadDB(sc,object);
			}
			
		});
	}
	
	public PEPersistentGroup findBalancedPersistentGroup(String prefix) {
		PersistentGroup uds = catalog.findBalancedPersistentGroup(prefix);
		
		return uds != null ? PEPersistentGroup.load(uds, this) : null;
	}


	public PESiteInstance findSiteInstance(Name n) {
		return schemaSource.find(this, PESiteInstance.getSiteInstanceKey(n));
	}

	public Database<?> findDatabase(Name n) {
		return schemaSource.find(this, PEDatabase.getDatabaseKey(n));
	}
	
	// used in tests
	public Database<?> findDatabase(String n) {
		return findDatabase(new UnqualifiedName(n));
	}
	
	public PEUser findUser(String name, String accessSpec) {
		return schemaSource.find(this, PEUser.getUserKey(name, accessSpec));
	}
	
	public PETenant findTenant(Name tenantName) {
		return schemaSource.find(this, PETenant.getTenantKey(tenantName));
	}

	public PEPriviledge findPrivilege(PEUser user, Name onName) {
		return schemaSource.find(this, PEPriviledge.getPriviledgeKey(user, onName));
	}

	public PEDatabase findPEDatabase(Name n) {
		return (PEDatabase)schemaSource.find(this, PEDatabase.getDatabaseKey(n));
	}
	
	public PEContainer findContainer(Name n) {
		return (PEContainer)schemaSource.find(this, PEContainer.getContainerKey(n));
	}
	
	public PEContainerTenant findContainerTenant(PEContainer cont, String disc) {
		return (PEContainerTenant)schemaSource.find(this, PEContainerTenant.getContainerTenantKey(cont, disc));
	}
	
	public PEAbstractTable<?> findTable(TableCacheKey tck) {
		return schemaSource.find(this, tck);
	}
	
	public TableScope findScope(ScopeCacheKey sck) {
		return schemaSource.find(this, sck);
	}
		
	public ListSet<TableScope> findScopesReferencing(PETable pet) {
		List<TableVisibility> matching = catalog.findTenantsOf(pet.getPersistent(this));
		ListSet<TableScope> out = new ListSet<TableScope>();
		for(TableVisibility tv : matching) 
			out.add(TableScope.load(tv, this));
		return out;
	}
	
	public List<String> findTenantTableNames(PETenant onTenant) {
		return catalog.findTenantTableNames(onTenant.getPersistent(this));
	}
	
	public List<PETable> findTablesWithUnresolvedFKSTargeting(UnqualifiedName dbName, UnqualifiedName tableName) {
		List<UserTable> unresolved = catalog.findTablesWithUnresolvedFKsTargeting(dbName.getUnquotedName().get(), tableName.getUnquotedName().get());
		final SchemaContext me = this;
		return Functional.apply(unresolved, new UnaryFunction<PETable,UserTable>() {

			@Override
			public PETable evaluate(UserTable object) {
				return PETable.load(object, me).asTable();
			}
			
		});
	}
	
	public MultiMap<PETable,PEForeignKey> findFKSReferencing(PETable pet) {
		List<UserTable> containingTables = catalog.findTablesWithFKSReferencing(pet.getPersistentID());
		MultiMap<PETable, PEForeignKey> out = new MultiMap<PETable, PEForeignKey>();
		for(UserTable ut : containingTables) {
			PETable opet = PETable.load(ut, this).asTable();
			for(PEForeignKey pefk : opet.getForeignKeys(this)) {
				if (pefk.getTargetTable(this) == pet)
					out.put(opet,pefk);
			}
		}
		return out;
	}
	
	// give me the scopes in the given tenant whose backing tables have forward foreign keys pointing to dbName.tableName
	public List<TableScope> findScopesWithUnresolvedFKsTargeting(UnqualifiedName dbName, UnqualifiedName tableName, PETenant onTenant) {
		List<TableVisibility> unresolved = catalog.findScopesWithUnresolvedFKsTargeting(dbName.getUnquotedName().get(), 
				tableName.getUnquotedName().get(), onTenant.getPersistentID());
		return loadScopes(unresolved);
	}
	
	// give me the scopes in the given tenant which represent the target tables of nonforward foreign keys of the given backing table.
	public List<TableScope> findScopesForFKTargets(PETable table, PETenant onTenant) {
		List<TableVisibility> refs = catalog.findScopesForFKTargets(table.getPersistentID(), onTenant.getPersistentID());
		return loadScopes(refs);
	}

	// give me the scopes in the given tenant which represent the backing tables that refer to the given backing table. 
	// (inverse of previous).
	public List<TableScope> findScopesForFKRefs(PETable backingTable, PETenant onTenant) {
		List<TableVisibility> refs = catalog.findScopesWithFKsReferencing(backingTable.getPersistentID(), onTenant.getPersistentID());
		return loadScopes(refs);
	}
	
	private List<TableScope> loadScopes(List<TableVisibility> in) {
		final SchemaContext me = this;
		return Functional.apply(in, new UnaryFunction<TableScope,TableVisibility>() {

			@Override
			public TableScope evaluate(TableVisibility object) {
				return TableScope.load(object, me);
			}
			
		});
		
	}
	
	public PEKey findForeignKey(Database<?> db, Integer tenantID, String name, String constraintName) throws PEException {
		Database<?> udb = db;
		if (db == null) {
			udb = getCurrentDatabase(false);
			if (udb == null) {
				return null;
			}
		}
		
		Key key = catalog.findForeignKey(udb.getPersistent(this), tenantID, name, constraintName);
		if (key == null) {
			return null;
		}
		PEKey peKey = PEKey.load(key, this, PEAbstractTable.load(key.getTable(), this).asTable());
		return peKey;
	}
		
	public Collection<PEProvider> findAllProviders() {
		return PEProvider.AllProvidersCacheKey.get(this);
	}
	
	public Collection<PEStorageSite> findAllPersistentSites() {
		return PEStorageSite.AllPersistentSitesCacheKey.get(this);
	}

	public Collection<PEPersistentGroup> findAllPersistentGroups() {
		return PEPersistentGroup.AllPersistentGroupsCacheKey.get(this);
	}

	public MultiMap<PEUser,PEPriviledge> findUsersForGenAdd() {
		List<User> users = catalog.findAllUsers();
		MultiMap<PEUser,PEPriviledge> out = new MultiMap<PEUser,PEPriviledge>();
		for(User u : users) {
			PEUser peu = PEUser.load(u, this);
			for(Priviledge p : u.getPriviledges()) {
				out.put(peu,PEPriviledge.load(p, this));
			}
		}
		return out;
	}
	
	public List<PETemplate> findMatchTemplates() {
		List<PersistentTemplate> any = catalog.findMatchTemplates();
		final SchemaContext me = this; 
		return Functional.apply(any, new UnaryFunction<PETemplate, PersistentTemplate>() {

			@Override
			public PETemplate evaluate(PersistentTemplate object) {
				return PETemplate.load(object, me);
			}
			
		});
	}
	
	public List<PEAbstractTable<?>> findTablesOnGroup(String groupName) {
		List<UserTable> tabs = catalog.findTablesOnGroup(groupName);
		final SchemaContext me = this;
		return Functional.apply(tabs, new UnaryFunction<PEAbstractTable<?>,UserTable>() {

			@Override
			public PEAbstractTable<?> evaluate(UserTable object) {
				return PEAbstractTable.load(object, me);
			}
			
		});
	}

	// catalog object collector, created for ddl purposes
	public static class PersistContext {
		
		private final Map<Persistable<?,?>, CatalogEntity> assoc;
		// ordering is important
		private final LinkedHashSet<CatalogEntity> objects;  //NOPMD
		
		// keep track of how many refs there are to the persist context, and when the refs fall to 0 
		// make sure we free it from the enclosing schema context
		private int refcount;
		
		public PersistContext() {
			this.assoc = new HashMap<Persistable<?,?>, CatalogEntity>();
			this.objects = new LinkedHashSet<CatalogEntity>();
			this.refcount = 0;
		}
		
		protected void incrementRefCount() {
			refcount++;
		}
		
		protected boolean decrementRefCount() {
			return (--refcount <= 0);
		}
		
		public void add(Persistable<?,?> from, CatalogEntity o) {
			assoc.put(from, o);
			objects.add(o);
		}
		
		public LinkedHashSet<CatalogEntity> getObjects() {  //NOPMD
			return objects;
		}
		
		public Map<Persistable<?,?>, CatalogEntity> getAssociation() {
			return assoc;
		}
	}

	// for dist key mapping
	public enum DistKeyOpType {
		QUERY, SELECT_FOR_UPDATE, UPDATE, INSERT;
	}

	// used when we don't have a connection, either transient impl or SSCon
	private static class NullConnectionContext implements ConnectionContext {

		private SchemaContext sc = null;
		private SchemaEdge<PEUser> root = null;
		private SchemaEdge<IPETenant> currentTenant = null;
		private SchemaEdge<Database<?>> db = null;
		
		private final CatalogDAO dao;
		
		public NullConnectionContext(CatalogDAO c) {
			dao = c;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public SchemaEdge<IPETenant> getCurrentTenant() {
			if (currentTenant == null)
				currentTenant = StructuralUtils.buildEdge(sc,(PETenant)null,true); 
			return currentTenant; 
		}

		@Override
		public boolean allowTenantColumnDecls() {
			return false;
		}

		@Override
		public void setSchemaContext(SchemaContext sc) {
			this.sc = sc;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void setCurrentTenant(IPETenant ten) {
			currentTenant = StructuralUtils.buildEdge(sc, ten, true);
		}

		@Override
		public String getVariableValue(AbstractVariableAccessor va) throws PEException {
			return null;
		}

		@Override
		public List<List<String>> getVariables(VariableScope vs)
				throws PEException {
			return null;
		}

		@SuppressWarnings("unchecked")
		@Override
		public SchemaEdge<PEUser> getUser() {
			if (root == null) {
				PEUser target = null;
				if (sc.getCatalog().isPersistent())
					target = PEUser.load(sc.getCatalog().findUsers(PEConstants.ROOT, null).get(0), sc);
				else
					target = new PEUser(sc);
				root = StructuralUtils.buildEdge(sc,target, sc.getCatalog().isPersistent());
			}
			return root;
		}

		@Override
		public SchemaEdge<Database<?>> getCurrentDatabase() {
			return db;
		}

		@Override
		public String getName() {
			return "NullConnectionContext";
		}

		@Override
		public void acquireLock(LockSpecification ls, LockType type) {
		}
		
		@Override
		public boolean isInTxn() {
			return false;
		}

		@Override
		public ConnectionContext copy() {
			return new NullConnectionContext(dao);
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
			return 0;
		}
		@Override
		public ConnectionMessageManager getMessageManager() {
			return new ConnectionMessageManager();
		}

		@Override
		public long getLastInsertedId() {
			return -1;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void setCurrentDatabase(Database<?> db) {
			this.db = StructuralUtils.buildEdge(sc, db, true);
		}

		@Override
		public CatalogDAO getDAO() {
			return dao;
		}

		@Override
		public boolean isInXATxn() {
			return false;
		}

		@Override
		public VariableStoreSource getVariableSource() {
			// TODO Auto-generated method stub
			return null;
		}
	}	
}
