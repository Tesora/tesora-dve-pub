package com.tesora.dve.common.catalog;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.LockModeType;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.sql.DataSource;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Statistics;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.C3P0Registry;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.distribution.RangeTableRelationship.RangeTableRelationshipCacheLookup;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.sql.util.Functional;

public class CatalogDAO {
	private static Logger logger = Logger.getLogger(CatalogDAO.class);

	AtomicReference<EntityManager> em = new AtomicReference<EntityManager>();

	int transactionDepth = 0;

	private CatalogDAO(EntityManager em) {
		this.em.set(em);
	}

	public void close() {
		EntityManager theEm = em.getAndSet(null);
		if (theEm != null && theEm.isOpen())
			theEm.close();
	}

	@Override
	public String toString() {
		return "CatalogDAO@" + System.identityHashCode(this);
	}

	public void begin() {
		if (transactionDepth++ == 0) {
			em.get().getTransaction().begin();
			// System.out.println("@" + System.identityHashCode(this) +
			// " beginning new txn");
		}
	}

	public void nonNestedBegin() {
		if (transactionDepth > 0)
			throw new PECodingException("Attempt to start non-nested transaction inside transaction scope");
		++transactionDepth;
		em.get().getTransaction().begin();
	}

	public Query createQuery(String queryString) {
		return em.get().createQuery(queryString);
	}

	public void commit() {
		if (transactionDepth == 1) {
			em.get().getTransaction().commit();
		}
		if (transactionDepth > 0)
			--transactionDepth;
	}

	public void rollbackNoException() {
		if (transactionDepth > 0)
			em.get().getTransaction().rollback();
		transactionDepth = 0;
	}

	public void rollback(Throwable t) {
		try {
			if (em.get().getTransaction().isActive())
				rollbackNoException();
		} catch (Throwable rollbackT) {
			logger.warn("Exception during rollback", rollbackT);
			throw new PersistenceException("Exception during rollback (check log for rollback exception)", t);
		}
	}

	public void retryableRollback(Throwable t) {
		try {
			if (em.get().getTransaction().isActive())
				rollbackNoException();
		} catch (Throwable rollbackT) {
			logger.warn("Exception during rollback", rollbackT);
			logger.warn("Original exception", t);
		}
	}

	public void cleanupRollback() {
		if (em.get().getTransaction().isActive())
			em.get().getTransaction().rollback();
		transactionDepth = 0;
	}

	public void persistToCatalog(Object o) {
		em.get().persist(o);
	}

	@SuppressWarnings("unchecked")
	public <T extends CatalogEntity> T findByKey(Class<? extends CatalogEntity> c, int id) {
		return (T) em.get().find(c, id);
	}

	@SuppressWarnings("unchecked")
	public <T extends CatalogEntity> T findByName(Class<? extends CatalogEntity> tClass, String entityName,
			boolean except) throws PENotFoundException {
		return (T) CatalogDAOFactory.INSTANCE.getLookupCache(tClass, "name").findByValue(this, entityName, except);

	}

	@SuppressWarnings("unchecked")
	private <T> List<T> findAllByClass(Class<T> entityClass) {
		Query q = em.get().createQuery("from " + entityClass.getSimpleName());
		return (List<T>) q.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<CatalogEntity> queryCatalogEntity(String queryStr) {
		return queryCatalogEntity(queryStr, Collections.EMPTY_MAP);
	}

	@SuppressWarnings("rawtypes")
	public List nativeQuery(String nativeQueryString, Map<String, Object> params) {
		Query q = em.get().createNativeQuery(nativeQueryString);
		for (Map.Entry<String, Object> me : params.entrySet())
			q.setParameter(me.getKey(), me.getValue());
		return q.getResultList();
	}

	@SuppressWarnings("rawtypes")
	public List nativeQuery(String nativeQueryString, Map<String, Object> params, Class<?> targ) {
		Query q = em.get().createNativeQuery(nativeQueryString, targ);
		for (Map.Entry<String, Object> me : params.entrySet())
			q.setParameter(me.getKey(), me.getValue());
		return q.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<CatalogEntity> queryCatalogEntity(String queryStr, Map<String, Object> params) {
		Query query = em.get().createQuery(queryStr);
		for (Map.Entry<String, Object> me : params.entrySet())
			query.setParameter(me.getKey(), me.getValue());
		return (List<CatalogEntity>) query.getResultList();
	}

	public List<User> findAllUsers() {
		return findAllByClass(User.class);
	}

	public List<DistributionModel> findAllDistributionModels() {
		return findAllByClass(DistributionModel.class);
	}

	public List<UserDatabase> findAllUserDatabases() {
		return findAllByClass(UserDatabase.class);
	}

	public List<Project> findAllProjects() {
		return findAllByClass(Project.class);
	}

	public List<Provider> findAllProviders() {
		return findAllByClass(Provider.class);
	}

	public List<DynamicPolicy> findAllDynamicPolicies() {
		return findAllByClass(DynamicPolicy.class);
	}

	public List<ExternalService> findAllExternalServices() {
		return findAllByClass(ExternalService.class);
	}

	public List<CharacterSets> findAllCharacterSets() {
		return findAllByClass(CharacterSets.class);
	}

	public List<Collations> findAllCollations() {
		return findAllByClass(Collations.class);
	}

	@SuppressWarnings("unchecked")
	public List<UserTable> findAllTablesInPersistentGroup(PersistentGroup sg) {
		Query query = em.get().createQuery("from UserTable ut where ut.persistentGroup = :storageGroup");
		query.setParameter("storageGroup", sg);

		return (List<UserTable>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public PersistentGroup findBalancedPersistentGroup(String prefix) {
		StringBuilder builder = new StringBuilder(
				"select pg.persistent_group_id, count(ud.user_database_id) as usecount from persistent_group pg left outer join user_database ud on pg.persistent_group_id = ud.default_group_id where pg.name not in ('InformationSchemaGroup','SystemGroup') ");
		if (StringUtils.isNotBlank(prefix))
			builder.append("and pg.name like '" + prefix + "%'");
		builder.append(" group by pg.persistent_group_id order by usecount asc, pg.persistent_group_id limit 1");

		Query query = em.get().createNativeQuery(builder.toString());

		List<Object[]> out = query.getResultList();
		if (out.isEmpty())
			return null;
		if (out.size() != 1)
			throw new IllegalStateException("Failed to find persistent group");
		Object[] first = out.get(0);
		return (PersistentGroup) findByKey(PersistentGroup.class, (Integer) first[0]);
	}

	@SuppressWarnings("unchecked")
	public List<UserDatabase> findAllMTDatabases() {
		Query query = em.get().createQuery("from UserDatabase ud where ud.multitenant_mode != 'off'");
		return (List<UserDatabase>) query.getResultList();
	}

	public DistributionModel findDistributionModel(String name) throws PEException {
		return findByName(DistributionModel.class, name, true);
	}

	public Map<String, DistributionModel> getDistributionModelMap() {
		Map<String, DistributionModel> modelMap = new HashMap<String, DistributionModel>();
		for (DistributionModel d : findAllByClass(DistributionModel.class))
			modelMap.put(d.getName(), d);
		return modelMap;
	}

	public PersistentSite createPersistentSite(String name, String url, String user, String password)
			throws PEException {
		SiteInstance si = new SiteInstance(name, url, user, password);
		PersistentSite db = new PersistentSite(name, si);
		em.get().persist(db);
		em.get().persist(si);
		return db;
	}

	public PersistentSite createPersistentSite(String siteName, String haType, SiteInstance master,
			SiteInstance[] replicants) throws PEException {
		PersistentSite theSite = new PersistentSite(siteName, master);
		theSite.setHaType(haType);
		em.get().persist(theSite);
		theSite.addAll(replicants);
		return theSite;
	}

	public PersistentGroup createPersistentGroup(String name) {
		PersistentGroup sc = new PersistentGroup(name);
		em.get().persist(sc);
		return sc;
	}

	public UserTable createUserTable(UserDatabase c, String name,
			DistributionModel dist, PersistentGroup sc, String engine, String tableType) {
		UserTable ut = new UserTable(name, dist, c, TableState.SHARED, engine, tableType);
		// dist.addUserTable(ut);
		ut.setPersistentGroup(sc);
		c.addUserTable(ut);
		em.get().persist(ut);
		return ut;
	}

	public User createUser(String name, String password, String accessSpec) {
		User u = new User(name, password, accessSpec);
		em.get().persist(u);
		return u;
	}

	public User createUser(String name, String password, String accessSpec, boolean adminUser) {
		User u = new User(name, password, accessSpec, adminUser);
		em.get().persist(u);
		return u;
	}

	public UserColumn createUserColumn(UserTable ut, String name, int dataType,
			String nativeTypeName, int size) {
		// TODO figure out best way to inject other column values
		// into this...
		UserColumn uc = new UserColumn(ut, name, dataType, nativeTypeName, size);

		uc.setAutoGenerated(false);
		uc.setNullable(true);
		uc.setHasDefault(false);

		return uc;
	}

	public UserDatabase createDatabase(String name, PersistentGroup sg, String charSet, String collation) {
		UserDatabase c = new UserDatabase(name, sg, null, TemplateMode.OPTIONAL, MultitenantMode.OFF, FKMode.STRICT, charSet, collation);
		em.get().persist(c);
		return c;
	}

	public Project createProject(String name) {
		Project p = new Project(name);
		em.get().persist(p);
		return p;
	}

	public Engines createEngines(String engine, String support, String comment, String transactions, String xa,
			String savepoints) {
		Engines engines = new Engines(engine, support, comment, transactions, xa, savepoints);
		em.get().persist(engines);
		return engines;
	}

	public Provider createProvider(String name, String plugin) {
		Provider prov = new Provider(name, plugin);
		em.get().persist(prov);
		return prov;
	}

	public Provider createProvider(String name, String plugin, String config) {
		Provider prov = new Provider(name, plugin, StringEscapeUtils.escapeSql(config));
		em.get().persist(prov);
		return prov;
	}
	
	public ExternalService createExternalService(String name, String plugin, String connectUser, boolean usesDataStore) throws PEException {
		return createExternalService(name, plugin, connectUser, usesDataStore, null);
	}

	public ExternalService createExternalService(String name, String plugin, String connectUser, boolean usesDataStore,
			String config) throws PEException {
		ExternalService extServ = new ExternalService(name, plugin, connectUser, usesDataStore);
		if (config != null)
			extServ.setConfig(config);
		em.get().persist(extServ);

		return extServ;
	}

	// -----------------------------------------------------------------
	// FIND Methods
	// 
	
	public List<PersistentGroup> findAllPersistentGroups() {
		return findAllByClass(PersistentGroup.class);
	}

	public List<UserTable> findAllUserTables() {
		return findAllByClass(UserTable.class);
	}

	public List<UserColumn> findAllUserColumns() {
		return findAllByClass(UserColumn.class);
	}

	public List<PersistentSite> findAllPersistentSites() {
		return findAllByClass(PersistentSite.class);
	}
	
	public PersistentGroup findDefaultPersistentGroup() throws PEException {
		return findByName(PersistentGroup.class, "Default", true);
	}

	public Project findDefaultProject() throws PEException {
		return findProject(Project.DEFAULT, true);
	}

	public Project findProject(String projectName) throws PEException {
		return findProject(projectName, true);
	}

	public Project findProject(String projectName, boolean except) throws PEException {
		return (Project) CatalogDAOFactory.INSTANCE.getLookupCache(Project.class, "name").findByValue(this,
				projectName, except);
	}

	public VariableConfig findVariableConfig(String variableName) throws PENotFoundException {
		return findVariableConfig(variableName, true);
	}
	
	public VariableConfig findVariableConfig(String variableName, boolean except) throws PENotFoundException {
		return (VariableConfig) CatalogDAOFactory.INSTANCE.getLookupCache(VariableConfig.class, "name").findByValue(this,
				variableName, except);
	}
	
	public List<VariableConfig> findAllVariableConfigs() {
		return findAllByClass(VariableConfig.class);
	}
	
	public UserDatabase findDatabase(String databaseName) throws PEException {
		return findDatabase(databaseName, true);
	}

	public UserDatabase findDatabase(String databaseName, boolean except) throws PEException {
		return findByName(UserDatabase.class, databaseName, except);
	}

	public PersistentGroup findPersistentGroup(String groupName) throws PEException {
		return findPersistentGroup(groupName, true);
	}

	public PersistentGroup findPersistentGroup(String groupName, boolean except) throws PEException {
		return findByName(PersistentGroup.class, groupName, except);
	}

	public PersistentSite findPersistentSite(String siteName) throws PEException {
		return findPersistentSite(siteName, true);
	}

	public PersistentGroup buildAllSitesGroup() throws PEException {
		List<PersistentSite> sites = findAllPersistentSites();
		final LinkedHashMap<String,PersistentSite> uniqueURLS = new LinkedHashMap<String,PersistentSite>();
		for(PersistentSite ps : sites) {
			String key = ps.getMasterUrl();
			PersistentSite already = uniqueURLS.get(key);
			if (already == null) 
				uniqueURLS.put(key,ps);
		}
		return new PersistentGroup(uniqueURLS.values());
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T onlyOne(List<T> results, String what, String searchedOn, boolean except)
			throws PENotFoundException {
		if (results.size() > 1 || (results.size() == 0 && except)) {
			throw new PENotFoundException("Expected exactly one " + what + " for name " + searchedOn);
		}
		if (results.size() == 0)
			return null;
		return (T) results.toArray()[0];
	}

	@SuppressWarnings("unchecked")
	public static <T> T onlyOne(List<? extends NamedCatalogEntity> results, String what, String dbid, String name,
			boolean except, boolean quoted) throws PENotFoundException {
		String searchedOn = dbid + ((name != null) ? ("/" + name) : "");

		if (results.size() == 0 && except) {
			throw new PENotFoundException("Expected exactly one " + what + " for name " + searchedOn);
		}
		if (results.size() == 0)
			return null;

		T ce = null;
		for (int i = 0; i < results.size(); i++) {
			T entity = (T) results.get(i);
			ce = findExactNameMatch(entity, name);
			if (ce != null) {
				break;
			}
		}
		if (quoted) {
			if (except && ce == null) {
				throw new PENotFoundException("Expected exactly one " + what + " for name " + searchedOn);
			}
			return ce;
		} else {
			// not quoted so if exact match then return it
			if (ce != null) {
				return ce;
			}
			// otherwise return any matching entity
			return (T) results.get(0);
		}
	}

	static private <T> T findExactNameMatch(T ce, String searchedOn) {
		T ret = null;
		// must be exact match
		if (StringUtils.equals(searchedOn, ((NamedCatalogEntity) ce).getName())) {
			ret = ce;
		}

		return ret;
	}

	public PersistentSite findPersistentSite(String siteName, boolean except) throws PEException {
		return findByName(PersistentSite.class, siteName, except);
	}

	public <T> T merge(T o) {
		return em.get().merge(o);
	}

	public <T> void detach(T o) {
		em.get().detach(o);
	}

	public boolean isDetached(Object o) {
		return !em.get().contains(o);
	}

	public <T> void remove(T o) {
		em.get().remove(o);
		CatalogDAOFactory.INSTANCE.clearLookupCache(o.getClass());
	}

	public <T> void refresh(T o) {
		try {
			em.get().refresh(o);
		} catch (IllegalArgumentException iae) {
			throw iae;
		}
	}

	public <T> void refreshForLock(T o) {
		em.get().refresh(o, LockModeType.PESSIMISTIC_WRITE);
	}

	public DistributionRange findRangeForTable(UserTable userTable) throws PEException {
		return findRangeTableRelationship(userTable, true).getRange();
	}

	public DistributionRange findRangeForTableByName(String qualifiedTableName, int tableId) throws PEException {
		return findRangeTableRelationshipByTableId(qualifiedTableName, tableId, true).getRange();
	}
	
	@SuppressWarnings("unchecked")
	public DistributionRange findRangeByName(String name, String groupName, boolean except) throws PEException {
		Query q = em.get().createQuery("from DistributionRange dr where dr.name = :name and dr.storageGroup.name = :groupName");
		q.setParameter("name", name);
		q.setParameter("groupName", groupName);
		List<DistributionRange> results = q.getResultList();
		return onlyOne(results, "DistributionRange", name + "/" + groupName, except); 
	}
	
	@SuppressWarnings("unchecked")
	public List<DistributionRange> findRangeByName(String name) throws PEException {
		Query q = em.get().createQuery("from DistributionRange dr where dr.name = :name");
		q.setParameter("name", name);
		return q.getResultList();
	}
	
	@SuppressWarnings("unchecked")
	public List<DistributionRange> findRangesOnGroup(String groupName) throws PEException {
		Query q = em.get().createQuery("from DistributionRange dr where dr.storageGroup.name = :groupName");
		q.setParameter("groupName", groupName);
		return q.getResultList();
	}

	// existence test, mostly
	public RangeTableRelationship findRangeTableRelationship(UserTable ut, boolean except) throws PEException {
		return (RangeTableRelationship) CatalogDAOFactory.INSTANCE
				.getLookupCache(RangeTableRelationship.class, "table").findByValue(this, ut.getQualifiedName(), ut,
						except);
	}

	public RangeTableRelationship findRangeTableRelationshipByTableId(String tableQualifiedName, int tableId,
			boolean except) throws PEException {
		return (RangeTableRelationship) CatalogDAOFactory.INSTANCE.getLookupCache(RangeTableRelationship.class,
				"table.id").findByValue(this, tableQualifiedName, tableId, except);
	}

	public Provider findProvider(String name) throws PEException {
		return findProvider(name, true);
	}

	public Provider findProvider(String name, boolean except) throws PEException {
		return findByName(Provider.class, name, except);
	}

	public DynamicPolicy findDynamicPolicy(String name) throws PEException {
		return findDynamicPolicy(name, true);
	}

	public DynamicPolicy findDynamicPolicy(String name, boolean except) throws PEException {
		return findByName(DynamicPolicy.class, name, except);
	}

	public ExternalService findExternalService(String name) throws PEException {
		return findExternalService(name, true);
	}

	public ExternalService findExternalService(String name, boolean except) throws PEException {
		return findByName(ExternalService.class, name, except);
	}

	@SuppressWarnings("unchecked")
	public List<User> findUsers(String name, String accessSpec) throws PEException {
		Query query = null;
		if (accessSpec == null)
			query = em.get().createQuery("from User u where u.name = :name");
		else {
			query = em.get().createQuery("from User u where u.name = :name and u.accessSpec = :accessSpec");
			query.setParameter("accessSpec", accessSpec);
		}
		query.setParameter("name", name);
		return (List<User>) query.getResultList();
	}

	public User findUser(String name, boolean except) throws PEException {
		List<User> candidates = findUsers(name, null);

		if (candidates.isEmpty()) {
			if (except)
				throw new PENotFoundException("User " + name + " not found in catalog");
			return null;
		}

		User catalogUser = null;
		catalogUser = candidates.get(0);
		if (candidates.size() > 1)
			logger.debug("More than one user found, choosing '" + catalogUser.getName() + "'@'"
					+ catalogUser.getAccessSpec() + "'");

		return catalogUser;
	}

	public User findUser(String name) throws PEException {
		return findUser(name, true);
	}

	public List<Tenant> findAllTenants() throws PEException {
		return findAllByClass(Tenant.class);
	}

	public Tenant findTenant(String extID) throws PEException {
		return findTenant(extID, true);
	}

	public Tenant findTenant(String extID, boolean except) throws PEException {
		return (Tenant) CatalogDAOFactory.INSTANCE.getLookupCache(Tenant.class, "externalID").findByValue(this, extID,
				except);
	}

	public List<TableVisibility> findAllTenantScopes() {
		return findAllByClass(TableVisibility.class);
	}

	public List<TableVisibility> findTenantScopesForTable(UserTable ut) {
		Query query = em.get().createQuery("from TableVisibility tv where tv.table = :ut");
		query.setParameter("ut", ut);
		@SuppressWarnings("unchecked")
		List<TableVisibility> res = query.getResultList();
		return res;
	}

	public TableVisibility findScopeForTable(UserTable ut, Tenant t, boolean except) throws PEException {
		Query query = em.get().createQuery("from TableVisibility tv where tv.table = :ut and tv.ofTenant = :t");
		query.setParameter("ut", ut);
		query.setParameter("t", t);
		@SuppressWarnings("unchecked")
		List<TableVisibility> res = query.getResultList();
		return onlyOne(res, "TenantVisibility", ut.getName() + "/" + t.getExternalTenantId(), except);
	}

	public TableVisibility findScopeForTable(int tableID, int tenantID, boolean except) throws PEException {
		Query query = em.get().createQuery(
				"from TableVisibility tv where tv.table.id = :tableID and tv.ofTenant.id = :tenantID");
		query.setParameter("tableID", tableID);
		query.setParameter("tenantID", tenantID);
		@SuppressWarnings("unchecked")
		List<TableVisibility> res = query.getResultList();
		return onlyOne(res, "TenantVisibility", tableID + "/" + tenantID, except);
	}

	// only used in adaptive mode - thus always has localName
	public TableVisibility findScope(int tenantID, String localName) throws PEException {
		Query query = em.get().createQuery(
				"from TableVisibility tv where tv.ofTenant.id = :tenantID and tv.localName = :localName");
		query.setParameter("tenantID", tenantID);
		query.setParameter("localName", localName);
		@SuppressWarnings("unchecked")
		List<TableVisibility> res = query.getResultList();
		return onlyOne(res, "TenantVisibility", localName + "/" + tenantID, true);
	}

	@SuppressWarnings("unchecked")
	public List<Priviledge> findPriviledgesOnDatabase(UserDatabase udb) throws PEException {
		Query query = em.get().createQuery("from Priviledge p where p.database = :udb");
		query.setParameter("udb", udb);
		return (List<Priviledge>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<Tenant> findTenantsOnDatabase(UserDatabase udb) throws Exception {
		Query query = em.get().createQuery("from Tenant t where t.userDatabase = :udb");
		query.setParameter("udb", udb);
		return (List<Tenant>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<ContainerTenant> findContainerTenants(Container ofContainer) throws Exception {
		Query query = em.get().createQuery("from ContainerTenant ct where ct.container = :cont");
		query.setParameter("cont", ofContainer);
		return (List<ContainerTenant>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<Priviledge> findPriviledgesOnTenant(Tenant t) throws PEException {
		Query query = em.get().createQuery("from Priviledge p where p.tenant = :t");
		query.setParameter("t", t);
		return (List<Priviledge>) query.getResultList();
	}

	public Priviledge findMatchingPrivilege(int userid, String name) throws PEException {
		// Query query =
		// em.createQuery("from Priviledge p where (p.database.name = :name or p.tenant.externalID = :name) and p.user = :user");
		Query query = em
				.get()
				.createQuery(
						"select p from Priviledge p left join fetch p.database ud left join fetch p.tenant t where p.user.id = :userid and (ud.name = :name or t.externalID = :name)");
		query.setParameter("userid", userid);
		query.setParameter("name", name);
		@SuppressWarnings("unchecked")
		List<Priviledge> res = query.getResultList();
		return onlyOne(res, "Privilege", userid + "/" + name, false);
	}

	public boolean findGlobalPrivilege(User user) throws PEException {
		Query query = em.get().createQuery(
				"from Priviledge p where p.user = :user and p.database is null and p.tenant is null");
		query.setParameter("user", user);
		@SuppressWarnings("unchecked")
		List<Priviledge> res = query.getResultList();
		Priviledge candidate = onlyOne(res, "Privilege", user.getName() + "/*", false);
		return candidate != null;
	}

	@SuppressWarnings("unchecked")
	public List<TableVisibility> findMatchingTenantRecords(UserTable ut) {
		Query query = em.get().createQuery("from TableVisibility tv where tv.table = :ut ");
		query.setParameter("ut", ut);
		return (List<TableVisibility>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<String> findTenantTableNames(Tenant t) {
		Query query = em.get().createQuery("select ut.name from UserTable ut where ut.name like :tname");
		query.setParameter("tname", "_" + t.getId() + "%");
		return (List<String>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<TableVisibility> findMatchingScopes(Shape matchShape, TableState matchState) throws PEException {
		Query query = em
				.get()
				.createQuery(
						"from TableVisibility tv where tv.table.state = :state and tv.table.shape = :shape order by tv.table.name");
		query.setParameter("shape", matchShape);
		query.setParameter("state", matchState);
		return (List<TableVisibility>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<UserTable> findMatchingTables(Shape matchShape, TableState matchState) throws PEException {
		Query query = em.get().createQuery(
				"from UserTable ut where ut.shape = :matchShape and ut.state = :matchState order by ut.name");
		query.setParameter("matchShape", matchShape);
		query.setParameter("matchState", matchState);
		return (List<UserTable>) query.getResultList();
	}

	public Shape findShape(UserDatabase indb, String logicalName, String definition, String typeHash)
			throws PEException {
		Query query = em
				.get()
				.createQuery(
						"from Shape s where s.userDatabase = :indb and s.tableDefinition = :definition and s.name = :logicalName and s.typeHash = :typeHash");
		query.setParameter("indb", indb);
		query.setParameter("definition", definition);
		query.setParameter("logicalName", logicalName);
		query.setParameter("typeHash", typeHash);
		@SuppressWarnings("unchecked")
		List<Shape> results = query.getResultList();
		return onlyOne(results, "Shape", indb.getName() + "/" + logicalName, false);
	}

	@SuppressWarnings("unchecked")
	public List<UserTable> findMatchingTables(UserDatabase indb, String logicalName, String definition) {
		Query query = em
				.get()
				.createQuery(
						"from UserTable ut where ut.userDatabase = :indb and ut.shape.tableDefinition = :definition and ut.shape.name = :logicalName and ut.stale != 1");
		query.setParameter("indb", indb);
		query.setParameter("definition", definition);
		query.setParameter("logicalName", logicalName);
		return (List<UserTable>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<Shape> findShapesOnDB(UserDatabase indb) {
		Query query = em.get().createQuery("from Shape s where s.userDatabase = :indb");
		query.setParameter("indb", indb);
		return (List<Shape>) query.getResultList();
	}

	public UserTable findUserTable(int dbid, String tableName, boolean quoted) throws PEException {
		Query query = em.get().createQuery("from UserTable ut where ut.name = :name and ut.userDatabase.id = :dbid");
		query.setParameter("name", tableName);
		query.setParameter("dbid", dbid);
		@SuppressWarnings("unchecked")
		List<UserTable> res = query.getResultList();
		return onlyOne(res, "UserTable", Integer.toString(dbid), tableName, false, quoted);

	}

	@SuppressWarnings("unchecked")
	public List<UserTable> findTablesOnGroup(String groupName) {
		Query query = em.get().createQuery("from UserTable ut where ut.persistentGroup.name = :groupName");
		query.setParameter("groupName",groupName);
		return query.getResultList();
	}
	
	@SuppressWarnings("unchecked")
	public UserView findView(String viewName, String dbName) throws PEException {
		Query query = em.get().createQuery(
				"from UserView uv where uv.table.name = :viewName and uv.table.userDatabase.name = :dbName");
		query.setParameter("viewName", viewName);
		query.setParameter("dbName", dbName);
		List<UserView> res = query.getResultList();
		return onlyOne(res, "UserView", dbName + "." + viewName, false);
	}

	public void recoverTransactions() {
//		DBConnectionParameters dbp = new DBConnectionParameters(Host.getProperties());
//		for (TransactionRecord txn : findAltemplate(TransactionRecord.class)) {
//			txn.recover2PC(this, dbp.getUserAuthentication());
//		}
	}

	@SuppressWarnings("unchecked")
	public List<UserTable> findContainerMemberTables(String containerName) throws PEException {
		Query query = em.get().createQuery("from UserTable ut where ut.container.name = :name");
		query.setParameter("name", containerName);
		return query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<PersistentTemplate> findMatchTemplates() throws PEException {
		Query query = em.get().createQuery("from PersistentTemplate pt where pt.dbmatch is not null order by pt.name");
		return query.getResultList();
	}

	public PersistentTemplate findTemplate(String name, boolean except) throws PEException {
		return findByName(PersistentTemplate.class, name, except);
	}
	
	public RawPlan findRawPlan(String name, boolean except) throws PEException {
		return findByName(RawPlan.class, name, except);
	}

	@SuppressWarnings("unchecked")
	public List<RawPlan> findAllEnabledRawPlans() throws PEException {
		Query q = em.get().createQuery("from RawPlan rp where rp.enabled = 1");
		return q.getResultList();
	}

	public abstract class EntityUpdater {
		public abstract CatalogEntity update() throws Throwable;

		/**
		 * A utility method that encapsulates the logic of updating a catalog
		 * entity
		 * 
		 * @return
		 * @throws Throwable
		 */
		public CatalogEntity execute() throws Throwable {
			CatalogEntity ret = null;
			begin();
			try {
				ret = update();
				commit();
			} catch (Throwable t) {
				rollback(t);
				throw t;
			}
			return ret;
		}
	}

	public abstract class EntityGenerator extends EntityUpdater {
		public abstract CatalogEntity generate() throws Throwable;

		@Override
		public CatalogEntity update() throws Throwable {
			CatalogEntity entity = generate();
			persistToCatalog(entity);
			return entity;
		}
	}

	public PersistentTemplate createTemplate(String name, String template, String match, String comment) {
		PersistentTemplate t = new PersistentTemplate(name, template, match, comment);
		em.get().persist(t);
		return t;
	}
	
	public SiteInstance createSiteInstance(String name, String url, String user, String password) {
		SiteInstance si = new SiteInstance(name, url, user, password);
		em.get().persist(si);
		return si;
	}
	
	public SiteInstance createSiteInstance(String name, String url, String user, String password, boolean isMaster,
			String status) {
		SiteInstance si = new SiteInstance(name, url, user, password, isMaster, status);
		em.get().persist(si);
		return si;
	}

	public SiteInstance findSiteInstance(String name) throws PEException {
		return findSiteInstance(name, true);
	}

	public SiteInstance findSiteInstance(String name, boolean except) throws PEException {
		return findByName(SiteInstance.class, name, except);
	}

	public UserTable loadTable(UserTable foo) {
		UserTable theTable = foo;
		if (!em.get().getEntityManagerFactory().getPersistenceUnitUtil().isLoaded(theTable, "userColumns")) {

			Query query = em.get().createQuery("from UserTable ut JOIN FETCH ut.userColumns WHERE ut.id = :id");
			query.setParameter("id", theTable.getId());
			theTable = (UserTable) query.getResultList().get(0);
		}
		return theTable;
	}

	@SuppressWarnings("unchecked")
	public List<ServerRegistration> findAllRegisteredServers() {
		Query query = em.get().createQuery("from ServerRegistration");
		return query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public ServerRegistration findServerRegistrationByAddress(String ourAddress) throws PENotFoundException {
		Query query = em.get().createQuery("from ServerRegistration where ipAddress = :ipAddress");
		query.setParameter("ipAddress", ourAddress);
		List<ServerRegistration> results = query.getResultList();
		return onlyOne(results, ourAddress, "ipAddress", true);
	}

	public Container findContainer(String containerName) throws PEException {
		return findContainer(containerName, true);
	}

	public Container findContainer(String containerName, boolean except) throws PEException {
		return findByName(Container.class, containerName, except);
	}

	@SuppressWarnings("unchecked")
	public ContainerTenant findContainerTenant(String containerName, String disc) throws PEException {
		Query query = em.get().createQuery(
				"from ContainerTenant ct where ct.container.name = :containerName and ct.discriminant = :disc");
		query.setParameter("containerName", containerName);
		query.setParameter("disc", disc);
		return onlyOne((List<ContainerTenant>) query.getResultList(), "ContainerTenant", containerName + "/" + disc,
				false);
	}

	@SuppressWarnings("unchecked")
	public List<UserTable> findBaseTables(UserDatabase within) throws PEException {
		Query query = em.get().createQuery(
				"from UserTable ut where ut.userDatabase = :udb and ut.container.baseTable = ut");
		query.setParameter("udb", within);
		return (List<UserTable>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<UserDatabase> findDatabasesWithin(Container cont) throws PEException {
		Query query = em.get().createQuery(
				"select distinct ut.userDatabase from UserTable ut where ut.container = :cont");
		query.setParameter("cont", cont);
		return (List<UserDatabase>) query.getResultList();
	}

	// this restricts the search to this server
	public List<TemporaryTable> findLocalUserlandTemporaryTables(Integer connID, String dbName, String tableName) {
		return findUserlandTemporaryTables(GroupManager.getCoordinationServices().getMemberAddress().toString(),connID,dbName, tableName);
	}
	
	// values not specified won't be queried on - so null,null,null returns all temp tables
	// and foo, null, null returns all temp tables on server foo
	@SuppressWarnings("unchecked")
	public List<TemporaryTable> findUserlandTemporaryTables(String serverName, Integer connID, String dbName, String tableName) {

		StringBuilder hql = new StringBuilder();
		
		hql.append("from TemporaryTable tt ");
		Query query = null;
		if (serverName == null && connID == null && tableName == null) {
			query = em.get().createQuery(hql.toString());
		} else {
			hql.append("where ");
			List<String> filters = new ArrayList<String>();
			if (serverName != null)
				filters.add("tt.server = :serverName");
			if (connID != null)
				filters.add("tt.sessionID = :connID");
			if (dbName != null)
				filters.add("tt.db = :dbName");
			if (tableName != null)
				filters.add("tt.name = :tableName");
			hql.append(Functional.join(filters, " and "));
			query = em.get().createQuery(hql.toString());
			if (serverName != null)
				query.setParameter("serverName",serverName);
			if (connID != null)
				query.setParameter("connID", connID);
			if (dbName != null)
				query.setParameter("dbName",dbName);
			if (tableName != null)
				query.setParameter("tableName", tableName);
		}
		return (List<TemporaryTable>)query.getResultList();
	}

	public void cleanupUserlandTemporaryTables(String serverName) {
		Query query = em.get().createQuery("delete from TemporaryTable where server = :name");
		query.setParameter("name", serverName);
		query.executeUpdate();
	}
	
	
	@SuppressWarnings("unchecked")
	public List<UserTable> findTablesWithUnresolvedFKsTargeting(String dbName, String tabName) {
		Query query = em
				.get()
				.createQuery(
						"select distinct k.userTable from Key k where k.constraint = 'FOREIGN' and k.referencedSchemaName = :dbName and k.referencedTableName = :tableName");
		query.setParameter("dbName", dbName);
		query.setParameter("tableName", tabName);
		return (List<UserTable>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<UserTable> findTablesWithFKsReferencing(int tabID) {
		Query query = em
				.get()
				.createQuery(
						"select distinct k.userTable from Key k where k.constraint = 'FOREIGN' and k.referencedTable.id = :tabID");
		query.setParameter("tabID", tabID);
		return (List<UserTable>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<TableVisibility> findScopesWithFKsReferencing(int tabID, int tenantID) {
		Query query = em.get().createQuery(
				"select tv from TableVisibility tv, Key k "
						+ "where k.constraint = 'FOREIGN' and k.referencedTable.id = :tabID "
						+ "and tv.ofTenant.id = :tenantID and k.userTable = tv.table");
		query.setParameter("tabID", tabID);
		query.setParameter("tenantID", tenantID);
		return (List<TableVisibility>) query.getResultList();
	}

	@SuppressWarnings("unchecked")
	public List<TableVisibility> findScopesWithUnresolvedFKsTargeting(String dbName, String tabName, int tenant) {
		/*
		 * I started with this sql: select s.scope_id from scope s inner join
		 * user_table ut on s.scope_table_id = ut.table_id inner join user_key
		 * uk on ut.table_id = uk.user_table_id where uk.forward_schema_name = ?
		 * and uk.forward_table_name = ? and s.scope_tenant_id = ?;
		 * 
		 * using this query, hql will cause us to deref the collection of keys 
		 * which would not be optimal
		 */
		Query query = em.get().createQuery(
				"select tv from TableVisibility tv, Key k "
						+ "where k.referencedSchemaName = :dbName and k.referencedTableName = :tabName "
						+ "and tv.ofTenant.id = :tenant " + "and tv.table = k.userTable");
		query.setParameter("tenant", tenant);
		query.setParameter("dbName", dbName);
		query.setParameter("tabName", tabName);
		return (List<TableVisibility>) query.getResultList();
	}

	// given a backing table and a tenant, tell me all the scopes in that tenant
	// of the tables it references.
	@SuppressWarnings("unchecked")
	public List<TableVisibility> findScopesForFKTargets(int tableID, int tenantID) {
		/*
		 * here is my raw query: select s.scope_id from user_key uk inner join
		 * scope s on s.scope_table_id = uk.user_table_id where
		 * uk.referenced_table = :tableID and s.scope_tenant_id = :tenantID
		 */
		Query query = em
				.get()
				.createQuery(
						"select tv from TableVisibility tv, Key uk where tv.table = uk.userTable and uk.referencedTable.id = :tableID and tv.ofTenant.id = :tenantID");
		query.setParameter("tableID", tableID);
		query.setParameter("tenantID", tenantID);
		return (List<TableVisibility>) query.getResultList();
	}

	public Key findForeignKey(UserDatabase db, Integer tenantID, String name, String constraintName, boolean except)
			throws PEException {
		if (StringUtils.isBlank(name) && StringUtils.isBlank(constraintName)) {
			return null;
		}
		StringBuffer qry = new StringBuffer("from Key k ");
		if (tenantID != null)
			qry.append(", TableVisibility tv ");
		qry.append("where k.constraint = 'FOREIGN' and k.userTable.userDatabase = :db ");
		if (!StringUtils.isBlank(name))
			qry.append("and k.name = :name");
		if (!StringUtils.isBlank(constraintName))
			qry.append("and k.constraintName = :constraintName");
		if (tenantID != null)
			qry.append(" and tv.ofTenant.id = :tenantID and tv.table = k.userTable ");
		Query query = em.get().createQuery(qry.toString());
		query.setParameter("db", db);
		if (!StringUtils.isBlank(name))
			query.setParameter("name", name);
		if (!StringUtils.isBlank(constraintName))
			query.setParameter("constraintName", constraintName);
		if (tenantID != null)
			query.setParameter("tenantID", tenantID);

		@SuppressWarnings("unchecked")
		List<Key> keys = query.getResultList();
		return onlyOne(keys, "Foreign Keys", name + "/" + constraintName, except);
	}

	public Key findKey(String dbName, String encTabName, String keyName) throws PEException {
		Query query = em
				.get()
				.createQuery(
						"from Key k where k.name = :keyName and k.userTable.name = :encTabName and k.userTable.userDatabase.name = :dbName");
		query.setParameter("dbName", dbName);
		query.setParameter("encTabName", encTabName);
		query.setParameter("keyName", keyName);
		@SuppressWarnings("unchecked")
		List<Key> keys = query.getResultList();
		return onlyOne(keys, "Specific key", dbName + "." + encTabName + "/" + keyName, false);
	}
	
	public void deleteAllServerRegistration() {
		Query q = em.get().createQuery("delete from ServerRegistration");
		q.executeUpdate();
	}
	
	
	// for autoupdate
	public EntityManager getEntityManager() {
		return em.get();
	}	
	
	public static DataSource getCatalogDS() {
		@SuppressWarnings("unchecked")
		Set<DataSource> c3p0pools = C3P0Registry.allPooledDataSources(); 
		return c3p0pools.iterator().next();
	}

	
	public enum CatalogDAOFactory {
		
		INSTANCE;
		
		EntityManagerFactory emf = null;
		
		Map<Class<?>, CachedCatalogLookup<? extends CatalogEntity>> lookupCacheMap
			= new HashMap<Class<?>, CachedCatalogLookup<? extends CatalogEntity>>();
		
		public static CatalogDAO newInstance() {
			return new CatalogDAO(INSTANCE.emf.createEntityManager());
		}

		public static CatalogDAO newInstance(Properties p) throws PEException {
			return new CatalogDAO(INSTANCE.getEMF(p).createEntityManager(fixProperties(p)));
		}
		
		public static boolean isSetup() {
			return INSTANCE.emf != null ? true : false;
		}
		public static void setup(Properties p) throws PEException {
			INSTANCE.emf = Persistence.createEntityManagerFactory("DveCatalog", fixProperties(p));
		}
		
		private static Properties fixProperties(Properties p) throws PEException {
			String url = p.getProperty(DBHelper.CONN_URL);
			
			// In production we don't have a default value for the catalog URL
			if(StringUtils.isEmpty(url)) 
				throw new PEException("Value for " + DBHelper.CONN_URL + " not specified in properties");

			Properties props = new Properties(p);
			props.putAll(p);
			
			PEUrl peUrl = PEUrl.fromUrlString(url);
			
			String dbName = props.getProperty(DBHelper.CONN_DBNAME);
			props.remove(DBHelper.CONN_DBNAME);
			peUrl.setPath(dbName);
			props.setProperty(DBHelper.CONN_URL, peUrl.getURL());
			props.remove("hibernate.hbm2ddl.auto");
			return props;
		}
		
		public static void shutdown() {
			if (Boolean.getBoolean("CatalogDAO.printCacheStatistics")) {
				CacheManager cm = CacheManager.getInstance();
				for (String s : cm.getCacheNames()) {
					if (s.matches("com.tesora.dve.*")) {
						Statistics stats = cm.getCache(s).getStatistics();
						System.out.println("Cache " + s + ": " + stats.getObjectCount() +
								" items (" + stats.getCacheHits() + " hits, " + stats.getCacheMisses() + " misses)");
					}
				}
			}
			INSTANCE.lookupCacheMap.clear();
			if (INSTANCE.emf != null)
				INSTANCE.emf.close();
			INSTANCE.emf = null;
		}
		
		public static void clearCache() {
			CacheManager.getInstance().clearAll();
		}
		
		EntityManagerFactory getEMF(Properties p) throws PEException {
			if (emf == null)
				setup(p);
			return emf;
		}
		
		public CachedCatalogLookup<? extends CatalogEntity> 
				getLookupCache(Class<? extends CatalogEntity> cacheClass, String columnName) {
			CachedCatalogLookup<? extends CatalogEntity> lookupCache = null;
			// it's possible to have a race condition here between threads within the same jvm
			// in particular, between a containsKey call and a get call the cache could be cleared
			// leading to npes in callers - so instead if we don't find a cache, create a new one
			// and return that
			lookupCache = lookupCacheMap.get(cacheClass);
			if (lookupCache == null) {
				lookupCache = new CachedCatalogLookup<CatalogEntity>(cacheClass, columnName);
				if (cacheClass.equals(RangeTableRelationship.class))
					lookupCache = new RangeTableRelationshipCacheLookup(cacheClass);
				lookupCacheMap.put(cacheClass, lookupCache);				
			}
			return lookupCache;
		}
		
		public void clearLookupCache(Class<?> cacheClass) {
			lookupCacheMap.remove(cacheClass);
		}
	}
}
