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


import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TenantIDLiteral;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.schema.mt.TenantColumn;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.statement.ddl.PEAlterTableStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTenantStatement;
import com.tesora.dve.sql.statement.ddl.PECreateDatabaseStatement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTableStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTenantStatement;
import com.tesora.dve.sql.statement.ddl.PEDropDatabaseStatement;
import com.tesora.dve.sql.statement.ddl.PEDropTableStatement;
import com.tesora.dve.sql.statement.ddl.PEDropTenantStatement;
import com.tesora.dve.sql.statement.ddl.alter.AbstractAlterColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTargetKind;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement.ValueHandler;
import com.tesora.dve.sql.statement.session.UseDatabaseStatement;
import com.tesora.dve.sql.statement.session.UseTenantStatement;
import com.tesora.dve.sql.template.TemplateManager;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variables.KnownVariables;

public class SchemaPolicyContext {

	public static SchemaPolicyContext buildContext(SchemaContext sc) {
		IPETenant tenant = sc.getCurrentTenant().get(sc);
		if (tenant instanceof PEContainerTenant)
			return new ContainerPolicyContext(sc,(PEContainerTenant)tenant);
		else if (tenant instanceof PETenant)
			return AdaptiveMultitenantSchemaPolicyContext.build(sc,((PETenant)tenant).getDatabase(sc));
		Database<?> db = sc.getCurrentDatabase(false,false);
		if (db == null || db.isInfoSchema())
			return new SchemaPolicyContext(sc);
		PEDatabase peds = (PEDatabase) db;
		if (peds.getMTMode().isMT())
			return AdaptiveMultitenantSchemaPolicyContext.build(sc, peds);
		return new SchemaPolicyContext(sc);
	}
	
	protected SchemaContext sc;
	protected SchemaEdge<PEUser> user;
	
	protected SchemaPolicyContext(SchemaContext cntxt) {
		sc = cntxt;
		user = sc.getCurrentUser();
	}

	protected SchemaContext getSchemaContext() {
		return sc;
	}
	
	public PEUser getUser() {
		return user.get(sc);
	}
	
	public void checkRootPermission(String what) {
		if (!isRoot())
			throw new SchemaException(Pass.SECOND, "You do not have permission to " + what);
	}
	
	public void checkEnabled() {
		checkEnabled(getCurrentTenant());
	}
	
	protected void checkEnabled(IPETenant tenant) {
		if (isRoot()) return;  // root user can always use a tenant
		if (tenant == null) return;
		if (tenant instanceof PETenant) {
			PETenant pet = (PETenant) tenant;
			if (pet.isSuspended())
				throw new SchemaException(Pass.SECOND, "Your account has been disabled");
		}
	}
	
	// use database statement needs to be mt aware
	// the lookup order is:
	// check for info schema - if info schema use that
	// 
	// find the user
	//   if user is not root
	//      find a visible db name/tenant in the user
	//   else
	//      find the tenant/db name on the catalog
	//      if dbname is an mtdb, set the tenant to the landlord and the db to the db
	//      if dbname is the landlord tenant, set the tenant to null and the db to the db
	
	public Statement buildUseDatabaseStatement(Name dbName) {
		if (PEConstants.INFORMATION_SCHEMA_DBNAME.equals(dbName.getCapitalized().get()) ||
			PEConstants.MYSQL_SCHEMA_DBNAME.equals(dbName.get())) {
			return new UseDatabaseStatement(sc.findDatabase(dbName));
		}
		PEUser currentUser = user.get(sc);
		if (!currentUser.isRoot()) {
			Persistable<?,?> any = currentUser.resolve(getSchemaContext(),dbName);
			if (any instanceof PEDatabase) {
				return new UseDatabaseStatement((PEDatabase)any);
			} else if (any instanceof PETenant) {
				PETenant ten = (PETenant) any;
				checkEnabled(ten);
				return new UseTenantStatement(ten,ten.getDatabase(sc));
			} else if (currentUser.hasGlobalPriviledge(getSchemaContext())) {
				Database<?> db = sc.findDatabase(dbName);
				if (db != null) {
					if (db.isInfoSchema()) return new UseDatabaseStatement(db);
					PEDatabase peds = (PEDatabase)db;
					if (!peds.getMTMode().isMT())
						return new UseDatabaseStatement(peds);
					// purposefully falling through because a regular user shouldn't be able to poke around
					// in the mtdb
				} else {
					PETenant tenant = sc.findTenant(dbName);
					if (tenant != null)
						return new UseTenantStatement(tenant,tenant.getDatabase(sc));					
				}
			}		
		} else {
			// since root can access everything, we have to look it up differently
			Database<?> db = sc.findDatabase(dbName);
			if (db != null) {
				if (db.isInfoSchema()) return new UseDatabaseStatement(db);
				PEDatabase peds = (PEDatabase) db;
				if (peds.getMTMode().isMT()) {
					PETenant pet = sc.findTenant(new UnqualifiedName(PEConstants.LANDLORD_TENANT));
					return new UseTenantStatement(pet,peds);
				}
				return new UseDatabaseStatement(peds);
			}
			if (PEConstants.LANDLORD_TENANT.equals(dbName.get())) {
				// setting to the pe landlord - don't set the tenant
				PEDatabase peds = sc.findSingleMTDatabase();
				return new UseDatabaseStatement(peds);
			}
			PETenant tenant = sc.findTenant(dbName);
			if (tenant != null)
				return new UseTenantStatement(tenant,tenant.getDatabase(sc));
		}
		throw new SchemaException(Pass.SECOND,"No such database: " + dbName.getSQL());
	}
	
	public Statement buildCreateTenantStatement(PEDatabase peds, Name tenantName, String description) {
		return buildCreateTenantStatement(peds, tenantName, description, "tenant", "Tenant", StatementType.UNIMPORTANT);
	}

	protected PETenant lookupTenant(Name n, boolean except) {
		PETenant any = sc.findTenant(n);
		if (any == null && except)
			throw new SchemaException(Pass.SECOND, "No such tenant: " + n.getSQL());
		return any;
	}
	
	protected Statement buildCreateTenantStatement(PEDatabase mtdb, Name tenantName, String description, String tag, String ltag, StatementType logicalType) {
		checkRootPermission("create a " + tag);
		PETenant existing = lookupTenant(tenantName, false);
		if (existing != null)
			throw new SchemaException(Pass.SECOND, ltag + " " + tenantName.getSQL() + " already exists");
		existing = new PETenant(sc,mtdb,tenantName.getUnquotedName().getSQL(), description);
		return new PECreateTenantStatement(existing,false, logicalType);
	}
	
	public Statement buildDropTenantStatement(Name tenantName, boolean viaDropDB) {
		checkRootPermission("drop a tenant");
		if (tenantName.get().toLowerCase().equals(PEConstants.LANDLORD_TENANT))
			throw new SchemaException(Pass.SECOND,"Cannot drop root tenant");
		PETenant existing = lookupTenant(tenantName, true);
		return new PEDropTenantStatement(null, existing, (viaDropDB ? StatementType.DROP_DB : null));
	}
	
	public Statement buildResumeTenantStatement(Name tenantName) {
		checkRootPermission("resume a tenant");
		PETenant existing = lookupTenant(tenantName, true);
		return new PEAlterTenantStatement(existing, false);
	}
	
	public Statement buildSuspendTenantStatement(Name tenantName) {
		checkRootPermission("suspend a tenant");
		PETenant existing = lookupTenant(tenantName, true);
		if (existing.getName().get().equals(PEConstants.LANDLORD_TENANT))
			throw new SchemaException(Pass.SECOND, "Cannot suspend root tenant");
		return new PEAlterTenantStatement(existing, true);
	}

	
	/**
	 * @param nt  
	 */
	public void modifyTablePart(PETable nt) {
	}
	
	public Statement modifyCreateTable(PECreateTableStatement stmt) {
		return stmt;
	}

	public Statement modifyDropTable(PEDropTableStatement stmt) {
		return stmt;
	}
	
	/**
	 * @param mustExist  
	 */
	public Database<?> getCurrentDatabase(Database<?> peds, boolean mustExist) {
		return peds;
	}
	
	/**
	 * dml rewrites occur for two cases:
	 * if an insert, then the tenant id MUST be set, and mt rewrites are required.
	 * if a delete/update/select, then the tenant id MAY be set, and mt rewrites MAY be required.
	 * 
	 * @param et  
	 */
	public boolean requiresMTRewrites(ExecutionType et) {
		return false;
	}
	
	public LiteralExpression getTenantIDLiteral(boolean mustExist) {
		Long value = getTenantID(mustExist);
		if (value == null) return null;
		sc.getValueManager().setTenantID(sc,value);
		return new TenantIDLiteral(sc.getValueManager());
	}
	
	public Long getTenantID(boolean mustExist) {
		if (mustExist)
			throw new SchemaException(Pass.REWRITER, "No tenant ids available in nontenant mode");
		return null;		
	}
	
	public IPETenant getCurrentTenant() {
		return null;		
	}
	
	public boolean showTenantColumn() {
		return false;
	}
	
	/**
	 * @param in  
	 */
	public void applyDegenerateMultitenantFilter(DMLStatement in) {
		// do nothing if it's not a mt context
	}
	
	public boolean allowTenantColumnDeclaration() {
		return false;
	}
	
	/**
	 * @param t  
	 */
	public void onDropUserTable(PETable t) {
	}

	public TableInstance buildInCurrentTenant(PESchema schema, UnqualifiedName n, LockInfo lockType) {
		return schema.buildInstance(getSchemaContext(),n, lockType, false);
	}
	
	// generally answers the question is it mt mode
	public boolean isMTMode() {
		return false;
	}
	
	public MultitenantMode getMTMode() {
		return MultitenantMode.OFF;
	}
	
	public boolean isRoot() {
		return user.get(sc).isRoot();
	}

	public boolean isSchemaTenant() {
		return false;
	}
	
	public boolean isDataTenant() {
		return false;
	}
	
	public boolean isAlwaysDistKey() {
		return false;
	}
	
	/**
	 * used in show create table
	 * 
	 * @param ut  
	 */
	public TableScope getOfTenant(UserTable ut) {
		return null;
	}
	
	public Name getLocalName(UserTable ut) {
		return new UnqualifiedName(ut.getName());
	}
	
	public long getNextAutoIncrBlock(TableInstance tab, long blockSize) {
		return sc.getCatalog().getNextIncrementValueChunk(sc,tab.getAbstractTable().asTable(), blockSize);
	}
	
	public long readAutoIncrBlock(TableKey tab) {
		return sc.getCatalog().readNextIncrementValue(sc,tab);
	}
	
	public void removeValue(TableKey tab, long value) {
		sc.getCatalog().removeNextIncrementValue(sc,tab, value);
	}
	
	public void removeValue(PETable tab, long value) {
		sc.getCatalog().removeNextIncrementValue(sc, tab, value);
	}
	
	/**
	 * @param ts 
	 * @param value  
	 */
	public void removeValue(TableScope ts, long value) {
		throw new SchemaException(Pass.PLANNER, "Invalid removeValue call: table scope in non mt context");
	}
	
	// when not in mt mode, we generally support all the usual alters
	// well, except if the target is a container base table - then we don't let you modify the discriminant columns
	public Statement modifyAlterTableStatement(PEAlterTableStatement in) {
		PETable targ = in.getTarget();
		if (targ.isContainerBaseTable(getSchemaContext())) {
			for(AlterTableAction aa : in.getActions()) {
				if (aa.getTargetKind() == AlterTargetKind.COLUMN) {
					AbstractAlterColumnAction aaca = (AbstractAlterColumnAction) aa;
					for(PEColumn targcol : aaca.getColumns()) {
						if (targcol.isPartOfContainerDistributionVector())
							throw new SchemaException(Pass.PLANNER, "Illegal alter on container base table " + targ.getName().getSQL() + " discriminant column " + targcol.getName().getSQL());
					}
				}
			}
		}
		return in;
	}
	
	private Statement buildCreatePEDatabase(Name dbName, Name defStorageGroup, Pair<Name, TemplateMode> templateDecl,
			Boolean ifNotExists, String tag, MultitenantMode mtm, FKMode fkm,
			String charSet, String collation) {
		assert (templateDecl != null);

		MultitenantMode mtmode = (mtm == null ? MultitenantMode.OFF : mtm);
		PEPersistentGroup pesg = null;
		if (defStorageGroup != null) {
			// An explicit persistent group has been specified - use it
			pesg = sc.findStorageGroup(defStorageGroup);
			if (pesg == null)
				throw new SchemaException(Pass.SECOND, "Persistent group " + defStorageGroup.getSQL() + " does not exist.");
		} else if (KnownVariables.BALANCE_PERSISTENT_GROUPS.getValue(sc.getConnection().getVariableSource()).booleanValue()) {
			// We need to dynamic pick a persistent group out of those configured
			pesg = sc.findBalancedPersistentGroup(
					KnownVariables.BALANCE_PERSISTENT_GROUPS_PREFIX.getValue(sc.getConnection().getVariableSource()));
			
			if(pesg == null)
				throw new SchemaException(Pass.SECOND, "Failed to find suitable persistent group for balanced database.");
		} else {
			// Use the default persistent group
			pesg = sc.getPersistentGroup();
			if (pesg == null) {
				throw new SchemaException(Pass.SECOND, "Must specify persistent group.  No default persistent group set on project.");
			}
		}

		final Pair<Name, TemplateMode> checkedTemplateDecl = TemplateManager.findTemplateForDatabase(sc, dbName, templateDecl.getFirst(),
				templateDecl.getSecond());
		
		PEDatabase pdb = new PEDatabase(sc, dbName.getUnquotedName(), pesg, checkedTemplateDecl, mtmode, fkm, charSet, collation);
		PECreateStatement<PEDatabase, UserDatabase> cdb = new PECreateDatabaseStatement(pdb, false, ifNotExists, tag, false);
		return cdb;		
	}
	
	public Statement buildCreateDatabase(Name dbName, Name defStorageGroup,  
			Pair<Name, TemplateMode> templateDecl, Boolean ifNotExists,
			String tag, MultitenantMode inmtm, FKMode fkm, 
			String charSet, String collation) {
		checkRootPermission("create a database");
		MultitenantMode mtm = (inmtm == null ? MultitenantMode.OFF : inmtm);
		// there's a few cases here:
		// [1] dbName is not a database, and not a tenant
		//    [a] there's a single mt database in mode adaptive - create a tenant
		//    [b] there's no single mt database - create a new database
		// [2] dbName is a database
		//    [a] dbName is currently differently declared - error
		//    [b] if not exists - empty create
		//    [c] error - dup database
		// [3] dbName is a tenant
		//    [a] if not exists - ok
		//    [b] error - dup tenant
		
		PEDatabase edb = sc.findPEDatabase(dbName);
		PETenant eten = sc.findTenant(dbName);
		if (edb == null && eten == null) {
			PEDatabase smt = sc.findSingleMTDatabase();
			if (smt == null) {
				return buildCreatePEDatabase(dbName,defStorageGroup,templateDecl,ifNotExists,tag,mtm,fkm,charSet,collation);
			}
			return buildCreateTenantStatement(smt,dbName, null, "database", "Database",StatementType.CREATE_DB);
		} else if (edb != null) {
			if (!edb.getMTMode().equals(mtm))
				throw new SchemaException(Pass.SECOND, 
						"Attempt to redeclare database " + dbName + " as a " + mtm.describe() 
						+ " database but " + dbName + " is a " + edb.getMTMode().describe() + " database");
			if (Boolean.TRUE.equals(ifNotExists)) 
				return new PECreateDatabaseStatement(edb,false,ifNotExists,tag,true);
			throw new SchemaException(Pass.SECOND, "Database " + dbName.getSQL() + " already exists");
		} else if (eten != null) {
			// redeclaration of tenant on same db is ok if if not exists
			PEDatabase smt = sc.findSingleMTDatabase();
			
			if (!smt.getName().equals(eten.getDatabase(sc).getName())) {
				throw new SchemaException(Pass.SECOND,"Attempt to redeclare a database with different backing storage");
			}
			if (Boolean.TRUE.equals(ifNotExists))
				return new PECreateTenantStatement(eten,ifNotExists,true,StatementType.CREATE_DB);
			throw new SchemaException(Pass.SECOND, "Database " + dbName.getSQL() + " already exists");
		} else {
			throw new SchemaException(Pass.SECOND, "Database " + dbName.getSQL() + " already exists");
		}
	}

	public Statement buildDropDatabaseStatement(Name dbName, Boolean ifExists, boolean dropmt, String tag) {
		checkRootPermission("drop a database");
		// We have a number of cases here
		// [1] dbName is an existing database
		//     [a] it's an mt database, and the dropmt flag is false - error
		//     [b] it's not an mt database, and the dropmt flag is true - error
		//     [c] drop the database
		// [2] dbName is an existing tenant
		//     [a] the tenant db is in standard mode - error
		//     [b] the tenant db is in relaxed/adaptive - drop the tenant
		// [3] dbName cannot be found
		//     [a] if not exists - empty statement
		//     [b] error
		PEDatabase eped = sc.findPEDatabase(dbName);
		PETenant eten = sc.findTenant(dbName);
		if (eped != null) {
			if (eped.getMTMode().isMT() && !dropmt)
				throw new SchemaException(Pass.SECOND, "Illegal drop database statement.  Use DROP MULTITENANT DATABASE to drop a multitenant database");
			else if (!eped.getMTMode().isMT() && dropmt)
				throw new SchemaException(Pass.SECOND, "Database " + dbName.getSQL() + " is not a multitenant database");
			return new PEDropDatabaseStatement(eped, tag);
		} else if (eten != null) {
			return buildDropTenantStatement(dbName,true);			
		} else {
			if (Boolean.TRUE.equals(ifExists))
				return new EmptyStatement("drop nonexistent database/tenant",StatementType.DROP_DB);
			throw new SchemaException(Pass.SECOND, "Database " + dbName.getSQL() + " does not exist");
		}
	}
	
	protected void addTenantColumn(PETable nt, UnqualifiedName tenantColumnName) {
		PEColumn already = nt.lookup(getSchemaContext(),tenantColumnName);
		if (already != null) {
			if (allowTenantColumnDeclaration())
				return;
			else
				throw new SchemaException(Pass.SECOND, "Tenant column conflict: found existing tenant column");
		} else {
			nt.addColumn(getSchemaContext(),new TenantColumn(getSchemaContext()));
		}
	}
	
	public boolean allowTenantColumnDeclarationChecking() {
		// allow two specific cases 
		//   - testing
		//   - alter support
		// note that we checked permissions already
		if (getSchemaContext().getConnection().allowTenantColumnDecls())
			return true;
		if (getSchemaContext().getOptions().isAllowTenantColumn())
			return true;
		return false;
	}

	public boolean isCacheableInsert(InsertIntoValuesStatement stmt) {
		PEAbstractTable<?> pet = stmt.getTableInstance().getAbstractTable();
		if (pet.isContainerBaseTable(getSchemaContext()))
			throw new SchemaException(Pass.NORMALIZE, "Inserts into base table "
					+ pet.getName().getSQL() 
					+ " for container " + pet.getDistributionVector(getSchemaContext()).getContainer(getSchemaContext()).getName().getSQL()
					+ " must be done when in the global container context");
		else if (pet.getDistributionVector(getSchemaContext()).isContainer()) 
			// insert into a cmt with no tenant specified is an error
			throw new SchemaException(Pass.NORMALIZE, "Inserts into table "
					+ pet.getName().getSQL()
					+ " for container " + pet.getDistributionVector(getSchemaContext()).getContainer(getSchemaContext()).getName().getSQL()
					+ " must be done when in a specific container context");			
		return true;		
	}

	public ValueHandler handleTenantColumnUponInsert(InsertIntoValuesStatement stmt, PEColumn column) {
		throw new SchemaException(Pass.NORMALIZE, "Tenant column found in non tenant mode");
	}	
	
	public boolean isContainerContext() {
		return false;
	}
}
