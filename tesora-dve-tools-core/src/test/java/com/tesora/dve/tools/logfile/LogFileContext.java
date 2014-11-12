package com.tesora.dve.tools.logfile;

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
import com.tesora.dve.common.catalog.TemporaryTable;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.UserTrigger;
import com.tesora.dve.common.catalog.UserView;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.infomessage.ConnectionMessageManager;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.CatalogContext;
import com.tesora.dve.sql.schema.ConnectionContext;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.Capability;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.cache.EntityCacheKey;
import com.tesora.dve.sql.schema.cache.PersistentCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.HollowTableVisibility;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.transexec.TransientGlobalVariableStore;
import com.tesora.dve.variables.AbstractVariableAccessor;
import com.tesora.dve.variables.GlobalVariableStore;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.LocalVariableStore;
import com.tesora.dve.variables.VariableManager;
import com.tesora.dve.variables.VariableStoreSource;
import com.tesora.dve.variables.VariableValueStore;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

// essentially similar to TransientExecutionEngine, except even more hollowed out
public class LogFileContext implements ConnectionContext, CatalogContext, VariableStoreSource {

	private SchemaContext tpc;

	private LocalVariableStore sessionVariables;
	private GlobalVariableStore globalVariables = new TransientGlobalVariableStore();
	private VariableValueStore userVariables = new VariableValueStore("User",false);
	private PEUser currentUser;
	
	public LogFileContext(NativeTypeCatalog types) {
		try {
			VariableManager.getManager().initialiseTransient(globalVariables);
			sessionVariables = globalVariables.buildNewLocalStore();
		} catch (PEException pe) {
			throw new SchemaException(Pass.FIRST, "Unable to initialize global vars for trans exec engine");
		}
		sessionVariables.setValue(KnownVariables.DYNAMIC_POLICY, OnPremiseSiteProvider.DEFAULT_POLICY_NAME);
		tpc = SchemaContext.createContext(this,this,types,Capability.PARSING_ONLY);
		currentUser = new PEUser(tpc);
	}

	@Override
	public boolean isPersistent() {
		return false;
	}

	@Override
	public void setContext(SchemaContext pc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CatalogContext copy(ConnectionContext cc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTempTableName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Project findProject(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Project createProject(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Project getDefaultProject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DynamicPolicy findPolicy(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistentGroup findPersistentGroup(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistentGroup createPersistentGroup(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PersistentGroup> findAllGroups() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistentGroup findBalancedPersistentGroup(String prefix) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistentSite createPersistentSite(String name, String haType,
			SiteInstance master, SiteInstance[] replicants) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistentSite createPersistentSite(String name, String url,
			String user, String password) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistentSite findPersistentSite(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PersistentSite> findAllSites() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SiteInstance createSiteInstance(String name, String url,
			String user, String password, boolean master, String status) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SiteInstance findSiteInstance(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void persistToCatalog(Object o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, DistributionModel> getDistributionModelMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends CatalogEntity> T findByKey(PersistentCacheKey pck) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object findByKey(Class<? extends CatalogEntity> c, int id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserDatabase findUserDatabase(String n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<UserDatabase> findAllUserDatabases() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<UserDatabase> findAllMTDatabases() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserTable findUserTable(Name name, int dbid, String dbn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserView findView(String name, String dbn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<UserTable> findTablesOnGroup(String groupName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserTable createTempTable(UserDatabase udb, String name,
			DistributionModel dist) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DistributionRange findDistributionRange(String name, String groupName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<DistributionRange> findDistributionRange(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RangeTableRelationship findRangeTableRelationship(UserTable ut) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addAutoIncrement(SchemaContext sc, PETable onTable, Long offset) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getNextIncrementValue(SchemaContext sc, PETable onTable) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNextIncrementValueChunk(SchemaContext sc, PETable ut,
			long blockSize) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNextIncrementValueChunk(SchemaContext sc, TableScope ts,
			long blockSize) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long readNextIncrementValue(SchemaContext sc, TableKey tk) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, TableKey tk) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc, PETable tab) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AutoIncrementTracker getBackingTracker(SchemaContext sc,
			TableScope ts) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, TableKey onTable,
			long value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, TableScope scope,
			long value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeNextIncrementValue(SchemaContext sc, PETable tab,
			long value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MappingSolution mapKey(SchemaContext sc, IKeyValue dk, Model model,
			DistKeyOpType op, PEStorageGroup pesg, ConnectionValues cv) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<CatalogEntity> query(String query, Map<String, Object> params) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<CatalogEntity> nativeQuery(String query,
			Map<String, Object> params, Class<?> targetClass) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List nativeQuery(String query, Map<String, Object> params) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<User> findUsers(String name, String accessSpec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<User> findAllUsers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Priviledge findPrivilege(SchemaContext sc, int userid, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean findGlobalPrivilege(SchemaContext sc, PEUser user) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Tenant findTenant(String extID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TableVisibility findVisibilityRecord(int tableID, int tenantID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TableVisibility findTableVisibility(String tenantName, int tenantID,
			String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HollowTableVisibility findHollowTableVisibility(String tenantName,
			int tenantID, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<TableVisibility> findTenantsOf(UserTable tab) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> findTenantTableNames(Tenant t) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<UserTable> findMatchingTables(UserDatabase udb,
			String logicalName, String definition) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Provider findProvider(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Provider> findAllProviders() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void refresh(CatalogEntity cat, boolean pessimistic) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ExternalService findExternalService(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExternalService createExternalService(String name, String plugin,
			String connectUser, boolean usesDataStore, String config)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EntityCacheKey buildCacheKey(CatalogEntity ce) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Container findContainer(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<UserTable> findContainerMembers(String containerName)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContainerTenant findContainerTenant(String containerName,
			String discriminant) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void saveContainerTenant(Container cont, String disc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<UserTable> findTablesWithUnresolvedFKsTargeting(
			String schemaName, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<UserTable> findTablesWithFKSReferencing(int tabID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Key findForeignKey(UserDatabase db, Integer tenantID, String name,
			String constraintName) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<TableVisibility> findScopesWithUnresolvedFKsTargeting(
			String schemaName, String tableName, int tenantID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<TableVisibility> findScopesWithFKsReferencing(int tableID,
			int tenantID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<TableVisibility> findScopesForFKTargets(int tableID,
			int tenantID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistentTemplate findTemplate(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PersistentTemplate> findMatchTemplates() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RawPlan findRawPlan(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<RawPlan> findAllEnabledRawPlans() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<TemporaryTable> findUserlandTemporaryTable(Integer connID,
			String dbName, String tabName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startTxn() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commitTxn() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackTxn() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCurrentTenant(IPETenant ten) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public SchemaEdge<PEUser> getUser() {
		return StructuralUtils.buildEdge(tpc,currentUser,false);
	}

	@Override
	public SchemaEdge<IPETenant> getCurrentTenant() {
		return StructuralUtils.buildEdge(tpc,null,true);
	}

	@Override
	public SchemaEdge<Database<?>> getCurrentDatabase() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCurrentDatabase(Database<?> db) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean allowTenantColumnDecls() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setSchemaContext(SchemaContext sc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getVariableValue(AbstractVariableAccessor va)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<List<String>> getVariables(VariableScope vs) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getConnectionId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void acquireLock(LockSpecification ls, LockType type) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isInTxn() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isInXATxn() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ConnectionContext copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasFilter() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFilteredTable(Name table) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean originatedFromReplicationSlave() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getCacheName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConnectionMessageManager getMessageManager() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getLastInsertedId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CatalogDAO getDAO() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VariableStoreSource getVariableSource() {
		return this;
	}

	@Override
	public LocalVariableStore getSessionVariableStore() {
		return sessionVariables;
	}

	@Override
	public GlobalVariableStore getGlobalVariableStore() {
		return globalVariables;
	}

	@Override
	public VariableValueStore getUserVariableStore() {
		return userVariables;
	}

	@Override
	public UserTrigger findTrigger(String name, String dbName) {
		// TODO Auto-generated method stub
		return null;
	}

}
