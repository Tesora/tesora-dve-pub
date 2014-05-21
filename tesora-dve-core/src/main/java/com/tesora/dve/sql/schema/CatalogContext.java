// OS_STATUS: public
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

import java.util.List;
import java.util.Map;

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
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.cache.EntityCacheKey;
import com.tesora.dve.sql.schema.cache.PersistentCacheKey;
import com.tesora.dve.sql.schema.mt.HollowTableVisibility;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public interface CatalogContext {
			
	public boolean isPersistent();
	public CatalogDAO getDAO();
	public void setContext(SchemaContext pc);
	
	public CatalogContext copy(ConnectionContext cc);
	
	public String getTempTableName();
	
	public Project findProject(String name);
	public Project createProject(String name);

	public Project getDefaultProject();
	
	public DynamicPolicy getDynamicGroupPolicy();
	
	public DynamicPolicy findPolicy(String name);
	
	public PersistentGroup findPersistentGroup(String name);
	public PersistentGroup createPersistentGroup(String name);
	
	public List<PersistentGroup> findAllGroups();
	public PersistentGroup findBalancedPersistentGroup(String prefix);
			
	public PersistentSite createPersistentSite(String name, String haType, SiteInstance master, SiteInstance[] replicants) throws PEException;
	public PersistentSite createPersistentSite(String name, String url, String user, String password) throws PEException;
	public PersistentSite findPersistentSite(String name);
	public List<PersistentSite> findAllSites();

	public SiteInstance createSiteInstance(String name, String url, String user, String password, boolean master, String status);
	public SiteInstance findSiteInstance(String name);
	
	public void persistToCatalog(Object o);
	public Map<String, DistributionModel> getDistributionModelMap();
	
	public <T extends CatalogEntity> T findByKey(PersistentCacheKey pck);
	public Object findByKey(Class<? extends CatalogEntity>c, int id);
			
	public UserDatabase findUserDatabase(String n);
	public List<UserDatabase> findAllUserDatabases();
	
	public List<UserDatabase> findAllMTDatabases();
	
	public UserTable findUserTable(Name name, int dbid, String dbn);
	public UserView findView(String name, String dbn);
	public List<UserTable> findTablesOnGroup(String groupName);
	
	public UserTable createTempTable(UserDatabase udb, String name, DistributionModel dist);
	
	public DistributionRange findDistributionRange(String name, String groupName);
	public List<DistributionRange> findDistributionRange(String name);
	public RangeTableRelationship findRangeTableRelationship(UserTable ut);
	
	public void addAutoIncrement(SchemaContext sc, PETable onTable, Long offset);
	public long getNextIncrementValue(SchemaContext sc, PETable onTable);
	public long getNextIncrementValueChunk(SchemaContext sc, PETable ut, long blockSize);
	public long getNextIncrementValueChunk(SchemaContext sc, TableScope ts, long blockSize);
	public long readNextIncrementValue(SchemaContext sc, TableKey tk);
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, TableKey tk);
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, PETable tab);
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, TableScope ts);
	
	public void removeNextIncrementValue(SchemaContext sc, TableKey onTable, long value);
	
	public void removeNextIncrementValue(SchemaContext sc, TableScope scope, long value);
	public void removeNextIncrementValue(SchemaContext sc, PETable tab, long value);
	
	public MappingSolution mapKey(SchemaContext sc, IKeyValue dk, Model model, DistKeyOpType op, PEStorageGroup pesg) throws PEException;
	
	public List<CatalogEntity> query(String query, Map<String, Object> params);
	
	// native query returning entities
	public List<CatalogEntity> nativeQuery(String query, Map<String, Object> params, Class<?> targetClass);
	
	// native query return list of object[]
	@SuppressWarnings("rawtypes")
	public List nativeQuery(String query, Map<String, Object> params);
	
	public List<User> findUsers(String name, String accessSpec);
	public List<User> findAllUsers();
	public Priviledge findPrivilege(SchemaContext sc, int userid, String name);
	public boolean findGlobalPrivilege(SchemaContext sc, PEUser user);

	public Tenant findTenant(String extID);
	public TableVisibility findVisibilityRecord(int tableID, int tenantID);
	public TableVisibility findTableVisibility(String tenantName, int tenantID, String tableName);
	public HollowTableVisibility findHollowTableVisibility(String tenantName, int tenantID, String tableName);
	public List<TableVisibility> findTenantsOf(UserTable tab);
	public List<String> findTenantTableNames(Tenant t);
	public List<UserTable> findMatchingTables(UserDatabase udb, String logicalName, String definition);
	
	public Provider findProvider(String name);
	public List<Provider> findAllProviders();
	
	public void refresh(CatalogEntity cat, boolean pessimistic);
	
	public ExternalService findExternalService(String name);

	public ExternalService createExternalService(String name, String plugin,
			String connectUser, boolean usesDataStore, String config)
			throws PEException;
	
	// entity cache keys are impl specific
	public EntityCacheKey buildCacheKey(CatalogEntity ce);
	
	public Container findContainer(String string);
	List<UserTable> findContainerMembers(String containerName) throws PEException;
	public ContainerTenant findContainerTenant(String containerName, String discriminant);
	// we allocate new container tenants during dml planning, not during execution, hence the method
	public void saveContainerTenant(Container cont, String disc);
	
	// for late resolution of fks
	public List<UserTable> findTablesWithUnresolvedFKsTargeting(String schemaName, String tableName);
	// for drop support
	public List<UserTable> findTablesWithFKSReferencing(int tabID);
	public Key findForeignKey(UserDatabase db, Integer tenantID, String name, String constraintName) throws PEException;
	
	// late resolution of fks for tenants
	public List<TableVisibility> findScopesWithUnresolvedFKsTargeting(String schemaName, String tableName, int tenantID);
	public List<TableVisibility> findScopesWithFKsReferencing(int tableID, int tenantID);
	public List<TableVisibility> findScopesForFKTargets(int tableID, int tenantID);
	
	public PersistentTemplate findTemplate(String name);
	
	public List<PersistentTemplate> findMatchTemplates();
	
	public RawPlan findRawPlan(String name);
	public List<RawPlan> findAllEnabledRawPlans();
	
	public void startTxn();
	public void commitTxn();
	public void rollbackTxn();
}