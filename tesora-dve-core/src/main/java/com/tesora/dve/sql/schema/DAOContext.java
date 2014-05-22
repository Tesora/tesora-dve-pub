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


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.common.catalog.ContainerTenant;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.PersistentTemplate;
import com.tesora.dve.common.catalog.Priviledge;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.RawPlan;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.UserView;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.cache.EntityCacheKey;
import com.tesora.dve.sql.schema.cache.PersistentCacheKey;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.schema.mt.HollowTableVisibility;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class DAOContext implements CatalogContext {
	
	private static final Logger logger = Logger.getLogger(DAOContext.class);
	
	private final ConnectionContext context;
	
	public DAOContext(ConnectionContext c) {
		context = c;
	}

	@Override
	public CatalogContext copy(ConnectionContext cc) {
		return new DAOContext(cc);
	}
	
	@Override
	public void setContext(SchemaContext pc) {
	}
	
	@Override
	public CatalogDAO getDAO() {
		return context.getDAO();
	}
	
	@Override
	public Project findProject(String name) {
		try {
			return getDAO().findProject(name, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}
	
	@Override
	public void persistToCatalog(Object o) {
		getDAO().persistToCatalog(o);
	}

	@Override
	public Map<String, DistributionModel> getDistributionModelMap() {
		return getDAO().getDistributionModelMap();
	}

	@Override
	public Project createProject(String name) {
		return getDAO().createProject(name);
	}

	@Override
	public PersistentGroup createPersistentGroup(String name) {
		return getDAO().createPersistentGroup(name);
	}

	@Override
	public PersistentSite createPersistentSite(String name, String haType, 
			SiteInstance master, SiteInstance[] replicants) throws PEException {
		return getDAO().createPersistentSite(name, haType, master, replicants);
	}
	
	@Override
	public PersistentSite createPersistentSite(String name, String url, String user, String password) throws PEException {
		return getDAO().createPersistentSite(name, url, user, password);
	}

	@Override
	public SiteInstance createSiteInstance(String name,
			String url, String user, String password, boolean master, String status) {
		return getDAO().createSiteInstance(name, url, user, password, master, status);
	}

	@Override
	public UserTable createTempTable(UserDatabase udb, String name, DistributionModel dist) {
		try {
			return UserTable.newTempTable(udb, null, name, dist);				
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}
	
	@Override
	public DistributionRange findDistributionRange(String name, String groupName) {
		try {
			return getDAO().findRangeByName(name, groupName, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public List<DistributionRange> findDistributionRange(String rangeName) {
		try {
			return getDAO().findRangeByName(rangeName);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}
	
	@Override
	public PersistentGroup findPersistentGroup(String name) {
		try {
			return getDAO().findPersistentGroup(name, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public PersistentSite findPersistentSite(String name) {
		try {
			return getDAO().findPersistentSite(name, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public SiteInstance findSiteInstance(String name) {
		try {
			return getDAO().findSiteInstance(name, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public UserDatabase findUserDatabase(String n) {
		try {
			return getDAO().findDatabase(n, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public UserTable findUserTable(Name name, final int dbid, String dbn) {
		final String rawName = name.get();
		final boolean quoted = name.isQuoted();
		SingleLoader<UserTable> loader = new SingleLoader<UserTable>() {

			@Override
			public UserTable query(CatalogDAO c) throws Throwable {
				return c.findUserTable(dbid, rawName, quoted);
			}

			@Override
			public boolean validate(UserTable obj) {
				return PETable.valid(obj);
			}
			
		};
		return loader.load(getDAO(), 10, "Unable to load " + dbn + "." + rawName); 
	}

	@Override
	public RangeTableRelationship findRangeTableRelationship(final UserTable ut) {
		SingleLoader<RangeTableRelationship> loader = new SingleLoader<RangeTableRelationship>() {

			@Override
			public RangeTableRelationship query(CatalogDAO c) throws Throwable {
				try {
					return c.findRangeTableRelationshipByTableId(ut.getQualifiedName(), ut.getId(), false);
				} catch (PENotFoundException nfe) {
					return null;
				}
			}

			@Override
			public boolean validate(RangeTableRelationship obj) {
				if (obj.getRange() == null)
					return false;
				return true;
			}
			
		};
 
		return loader.load(getDAO(), 10, "Unable to load range table relationship for " + ut.getQualifiedName());
	}

	@Override
	public boolean isPersistent() {
		return true;
	}

	@Override
	public void addAutoIncrement(SchemaContext sc, PETable onTable, Long offset) {
		if (!onTable.getPersistent(sc).hasAutoIncr()) 
			onTable.getPersistent(sc).addAutoIncr(offset);
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, TableKey tk) {
		if (tk instanceof MTTableKey) {
			MTTableKey mttk = (MTTableKey) tk;
			return mttk.getScope().getPersistent(sc).getAutoIncr();
		} else {
			return tk.getAbstractTable().getPersistent(sc).getAutoIncr();
		}
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, PETable tab) {
		return tab.getPersistent(sc).getAutoIncr();
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, TableScope tab) {
		return tab.getPersistent(sc).getAutoIncr();
	}

	
	
	@Override
	public long getNextIncrementValue(SchemaContext sc, PETable onTable) {
		return AutoIncrementTracker.getNextValue(getDAO(), onTable.getAutoIncrTrackerID());
	}

	@Override
	public long getNextIncrementValueChunk(SchemaContext sc, PETable onTable, long blockSize) {
		return AutoIncrementTracker.getIdBlock(getDAO(), onTable.getAutoIncrTrackerID(), blockSize);
	}

	@Override
	public long getNextIncrementValueChunk(SchemaContext sc, TableScope ts, long blockSize) {
		return AutoIncrementTracker.getIdBlock(getDAO(), ts.getAutoIncrementID(), blockSize);
	}
	
	@Override
	public String getTempTableName() {
		return UserTable.getNewTempTableName();
	}

	@Override
	public MappingSolution mapKey(SchemaContext sc, IKeyValue kv, Model model,
			DistKeyOpType op, PEStorageGroup onGroup) throws PEException {
		MappingSolution mappingSolution = null;
		DistributionModel dm = null;
		dm = kv.getDistributionModel();
		if (op == DistKeyOpType.QUERY || op == DistKeyOpType.SELECT_FOR_UPDATE) {
			mappingSolution= dm.mapKeyForQuery(getDAO(), onGroup.getPersistent(sc), kv, op);
		} else if (op == DistKeyOpType.INSERT) {
			mappingSolution= dm.mapKeyForInsert(getDAO(), onGroup.getPersistent(sc), kv);
		} else if (op == DistKeyOpType.UPDATE) {
			mappingSolution= dm.mapKeyForInsert(getDAO(), onGroup.getPersistent(sc), kv);
		} else
			throw new PEException("Unknown mapKey operation type: " + op);
		return mappingSolution;			
	}

	@Override
	public List<CatalogEntity> query(String query,
			Map<String, Object> params) {
		return getDAO().queryCatalogEntity(query, params);
	}

	@Override
	public Project getDefaultProject() {
		try {
            return getDAO().findProject(Singletons.require(HostService.class).getDefaultProjectName());
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public Object findByKey(Class<? extends CatalogEntity> c, int id) {
		return getDAO().findByKey(c, id);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends CatalogEntity> T findByKey(PersistentCacheKey pck) {
		return getDAO().findByKey((Class<? extends CatalogEntity>) pck.getPersistentClass(), pck.getId());
	}
	
	@Override
	public List<User> findUsers(String name, String accessSpec) {
		try {
			return getDAO().findUsers(name,accessSpec);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public Tenant findTenant(String extID) {
		try {
			return getDAO().findTenant(extID, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public List<UserDatabase> findAllUserDatabases() {
		return getDAO().findAllUserDatabases();
	}

	@Override
	public List<PersistentGroup> findAllGroups() {
		return getDAO().findAllPersistentGroups();
	}

	@Override
	public PersistentGroup findBalancedPersistentGroup(String prefix) {
		return getDAO().findBalancedPersistentGroup(prefix);
	}

	@Override
	public TableVisibility findVisibilityRecord(int tableID, int tenantID) {
		try {
			return getDAO().findScopeForTable(tableID, tenantID, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public Provider findProvider(String name) {
		try {
			return getDAO().findProvider(name, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public List<Provider> findAllProviders() {
		return getDAO().findAllProviders();
	}

	@Override
	public DynamicPolicy getDynamicGroupPolicy() {
		return getDefaultProject().getDefaultPolicy();
	}

	@Override
	public DynamicPolicy findPolicy(String name) {
		try {
			return getDAO().findDynamicPolicy(name, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<CatalogEntity> nativeQuery(String query,
			Map<String, Object> params, Class<?> targetClass) {
		return getDAO().nativeQuery(query, params, targetClass);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List nativeQuery(String query, Map<String, Object> params) {
		return getDAO().nativeQuery(query, params);
	}
		
	@Override
	public List<PersistentSite> findAllSites() {
		return getDAO().findAllPersistentSites();
	}

	@Override
	public List<UserDatabase> findAllMTDatabases() {
		return getDAO().findAllMTDatabases();
	}

	@Override
	public List<TableVisibility> findTenantsOf(UserTable tab) {
		return getDAO().findMatchingTenantRecords(tab);
	}

	@Override
	public List<String> findTenantTableNames(Tenant t) {
		return getDAO().findTenantTableNames(t);
	}

	@Override
	public long readNextIncrementValue(SchemaContext sc, TableKey tk) {
		return getBackingTracker(sc,tk).readNextValue(getDAO());
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, TableKey tk, long value) {
		AutoIncrementTracker.removeValue(getDAO(), tk.getAutoIncTrackerID(), value);
	}
	
	@Override
	public void removeNextIncrementValue(SchemaContext sc, PETable tb, long value) {
		AutoIncrementTracker.removeValue(getDAO(), tb.getAutoIncrTrackerID(), value);
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, TableScope ts, long value) {
		AutoIncrementTracker.removeValue(getDAO(), ts.getAutoIncrementID(), value);
	}
	
	@Override
	public void refresh(CatalogEntity cat, boolean pessimistic) {
		if (pessimistic)
			getDAO().refreshForLock(cat);
		else
			getDAO().refresh(cat);
	}

	@Override
	public List<UserTable> findMatchingTables(UserDatabase udb,
			String logicalName, String definition) {
		return getDAO().findMatchingTables(udb, logicalName, definition);
	}
	
	@Override
	public ExternalService findExternalService(String name) {
		ExternalService es = null;
		try {
			es = getDAO().findExternalService(name, false);
		} catch (PEException pe) {
			es = null;
		}
		
		return es;
	}

	@Override
	public ExternalService createExternalService(String name, String plugin,
			String connectUser, boolean usesDataStore, String config) throws PEException {
		return getDAO().createExternalService(name, plugin, connectUser, usesDataStore, config);
	}

	@Override
	public Priviledge findPrivilege(SchemaContext sc, int userid, String name) {
		try {
			return getDAO().findMatchingPrivilege(userid, name);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public boolean findGlobalPrivilege(SchemaContext sc, PEUser user) {
		try {
			return getDAO().findGlobalPrivilege(user.getPersistent(sc));
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public EntityCacheKey buildCacheKey(CatalogEntity ce) {
		return new PersistentCacheKey(ce.getClass(),ce.getId());
	}

	@Override
	public TableVisibility findTableVisibility(String tenantName, int tenantID,
			String tableName) {
		final HashMap<String,Object> params = new HashMap<String,Object>();
		params.put("n",tableName);
		String tenantTest = null;
		if (tenantID < 1) {
			// search by name only
			tenantTest = "tv.ofTenant.externalID = :tenantName";
			params.put("tenantName",tenantName);
		} else {
			tenantTest = "tv.ofTenant.id = :tenantID";
			params.put("tenantID",tenantID);
		}
		final String query = "from TableVisibility tv where " + tenantTest + " and tv.localName = :n";
		SingleLoader<TableVisibility> loader = new SingleLoader<TableVisibility>() {

			@Override
			public TableVisibility query(CatalogDAO c) {
				List<CatalogEntity> results = c.queryCatalogEntity(query, params);
				if (results.isEmpty())
					return null;
				TableVisibility tv = (TableVisibility) results.get(0);
				c.refreshForLock(tv.getTable());
				return tv;
			}

			@Override
			public boolean validate(TableVisibility obj) {
				return TableScope.valid(obj);
			}
			
		};
		return loader.load(getDAO(), 10, 
				"Unable to load persistent scope for " + tableName + " of tenant " + tenantName,
				"Unable to load scope");
	}

	@Override
	public Container findContainer(String name) {
		try {
			return getDAO().findContainer(name, false);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public List<UserTable> findContainerMembers(String containerName) throws PEException {
		return getDAO().findContainerMemberTables(containerName);
	}

	@Override
	public ContainerTenant findContainerTenant(String containerName, String discriminantValue) {
		try {
			return getDAO().findContainerTenant(containerName, discriminantValue);
		} catch (PEException pe) {
			throw new RuntimeException(pe);
		}
	}

	@Override
	public void saveContainerTenant(Container cont, String disc) {
		ContainerTenant ct = new ContainerTenant(cont,disc);
		getDAO().begin();
		getDAO().persistToCatalog(ct);
		getDAO().commit();
	}

	@Override
	public List<UserTable> findTablesWithUnresolvedFKsTargeting(
			String schemaName, String tableName) {
		return getDAO().findTablesWithUnresolvedFKsTargeting(schemaName, tableName);
	}

	@Override
	public List<UserTable> findTablesWithFKSReferencing(int tabID) {
		return getDAO().findTablesWithFKsReferencing(tabID);
	}

	@Override
	public Key findForeignKey(UserDatabase db, Integer tenantID, String name, String constraintName) throws PEException {
		return getDAO().findForeignKey(db, tenantID, name, constraintName, false);
	}
	
	@Override
	public void startTxn() {
		getDAO().begin();
	}

	@Override
	public void commitTxn() {
		getDAO().commit();
	}

	@Override
	public void rollbackTxn() {
		getDAO().rollbackNoException();
	}

	// under high concurrent tenant load we sometimes have difficulties getting correctly populated objects
	// so add a retry
	private static abstract class SingleLoader<T> {
		
		public abstract T query(CatalogDAO c) throws Throwable;
		
		public abstract boolean validate(T obj);
		
		public T load(CatalogDAO c, int maxAttempts, String message) {
			return load(c, maxAttempts, message, message);
		}
		
		public T load(CatalogDAO c, int maxAttempts, String logWarnMessage, String unretryableMessage) {
			T obj = null;
			int attempts = 0;
			while(obj == null && attempts < maxAttempts) {
				attempts++;
				boolean rolledBack = false;
				try {
					c.begin();
					obj = query(c);
					if (obj == null) return null;
					if (!validate(obj)) {
						obj = null;
						continue;
					}
				} catch (Throwable t) {
					logger.warn(logWarnMessage,t);
					if (!AdaptiveMultitenantSchemaPolicyContext.canRetry(t))
						throw new SchemaException(Pass.SECOND, unretryableMessage, t);
					rolledBack = true;
					c.retryableRollback(t);
				} finally {
					if (!rolledBack) try {
						c.commit();
					} catch (Throwable t) {
						try {
							c.rollbackNoException();
						} catch (Throwable it) {
							
						}
					}
				}
			}
			return obj;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public HollowTableVisibility findHollowTableVisibility(String tenantName, int tenantID, String tableName) {
		String sql = 
				"select db.name as dbname, db.user_database_id, tab.name as tabname, ai.incr_id as incrid, s.scope_id as sid "
				+"from scope s inner join user_table tab on s.scope_table_id = tab.table_id "
				+"inner join user_database db on tab.user_database_id = db.user_database_id "
				+"left outer join auto_incr ai on s.scope_id = ai.scope_id "
				+"where s.scope_tenant_id = :tenantid and s.local_name = :localname";
		Map<String,Object> params = new HashMap<String,Object>();
		params.put("tenantid",tenantID);
		params.put("localname",tableName);
		List<Object> results = getDAO().nativeQuery(sql, params);
		if (results == null || results.isEmpty()) return null;
		if (results.size() > 1)
			throw new IllegalStateException("More than one hollow scope found for " + tenantName + "." + tableName);
		Object[] values = (Object[]) results.get(0);
		HollowTableVisibility htv = new HollowTableVisibility();
		htv.setDbName((String)values[0]);
		htv.setTabName((String)values[2]);
		Object dbid = values[1];
		Object incrid = values[3];
		Object scopeid = values[4];
		htv.setDbid(getRawID(dbid,false,"db id"));
		htv.setAutoIncrementId(getRawID(incrid,true,"auto_incr id"));
		htv.setScopeId(getRawID(scopeid,true,"scope id"));
		htv.setLocalName(tableName);
		htv.setTenantID(tenantID);
		htv.setTenantName(tenantName);
		return htv;
	}
	
	private static Integer getRawID(Object o, boolean nullok, String represents) {
		if (o instanceof Number) {
			Number n = (Number)o;
			return n.intValue();
		} else if (nullok) {
			return null;
		} else {
			throw new IllegalStateException("Unexpected " + represents + " value: " + o);
		}
	}

	@Override
	public PersistentTemplate findTemplate(String name) {
		try {
			return getDAO().findByName(PersistentTemplate.class, name, false);
		} catch (PEException pe) {
			throw new RuntimeException("Unable to find template " + name, pe);
		}
	}

	@Override
	public List<PersistentTemplate> findMatchTemplates() {
		try {
			return getDAO().findMatchTemplates();
		} catch (PEException pe) {
			throw new RuntimeException("Unable to find match templates",pe);
		}
	}

	@Override
	public RawPlan findRawPlan(String name) {
		try {
			return getDAO().findRawPlan(name, false);
		} catch (PEException pe) {
			throw new RuntimeException("Unable to find raw plan",pe);
		}
	}

	@Override
	public List<RawPlan> findAllEnabledRawPlans() {
		try {
			return getDAO().findAllEnabledRawPlans();
		} catch (PEException pe) {
			throw new RuntimeException("Unable to find enabled raw plans",pe);
		}
	}

	@Override
	public List<TableVisibility> findScopesWithUnresolvedFKsTargeting(String schemaName, String tableName, int tenantID) {
		return getDAO().findScopesWithUnresolvedFKsTargeting(schemaName, tableName, tenantID);
	}

	@Override
	public List<TableVisibility> findScopesWithFKsReferencing(int tableID, int tenantID) {
		return getDAO().findScopesWithFKsReferencing(tableID, tenantID);
	}

	@Override
	public List<TableVisibility> findScopesForFKTargets(int tableID, int tenantID) {
		return getDAO().findScopesForFKTargets(tableID, tenantID);
	}

	@Override
	public UserView findView(String name, String dbn) {
		try {
			return getDAO().findView(name,dbn);
		} catch (PEException pe) {
			throw new RuntimeException("Unable to find view",pe);
		}
	}

	@Override
	public List<UserTable> findTablesOnGroup(String groupName) {
		return getDAO().findTablesOnGroup(groupName);
	}

	@Override
	public List<User> findAllUsers() {
		return getDAO().findAllUsers();
	}
}