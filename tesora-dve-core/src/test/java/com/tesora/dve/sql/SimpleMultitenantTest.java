package com.tesora.dve.sql;

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
// NOPMD by doug on 04/12/12 12:05 PM





import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.schema.mt.TenantColumn;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.variable.VariableConstants;

public class SimpleMultitenantTest extends MultitenantTest {

	@Test
	public void testMultitenantCommands() throws Throwable {
		setContext("testMultitenantCommands");
		// verify that we can't create a tenant
		try {
			createTenant(0);
			fail("shouldn't be able to create tenants when not in multitenant mode");
		} catch (Exception e) {
			assertException(e, SQLException.class,
					"Internal error: No multitenant database found");
		}
		// create the database - for this test we use relaxed
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		// let's try creating a tenant
		createTenant(0);
		rootConnection.assertResults("show tenants", 
				br(nr, AdaptiveMultitenantSchemaPolicyContext.LANDLORD_TENANT, testDDL.getDatabaseName(),"NO",getIgnore(),
				   nr,tenantNames[0],testDDL.getDatabaseName(),"NO", "mtt"));
		try {
			tenantConnection.execute("create tenant dg2 'second dg user'");
			fail("tenant shouldn't be able to create tenants");
		} catch (Exception e) {
			assertException(e, SQLException.class,
					"Internal error: You do not have permission to create a tenant");
		}
		try {
			tenantConnection.execute("show tenants");
			fail("tenant shouldn't be able to show tenants");
		} catch (Exception e) {
			assertException(e, SQLException.class,
					"Internal error: You do not have permission to show tenants");
		}
		// create the second tenant now via the create database statement
		createTenant(1);
		tenantConnection.execute("use " + tenantNames[0]);
		rootConnection.execute("suspend tenant " + tenantNames[0]);
		rootConnection.assertResults("show tenants",
				br(nr,AdaptiveMultitenantSchemaPolicyContext.LANDLORD_TENANT,testDDL.getDatabaseName(),"NO",getIgnore(),
				   nr,tenantNames[0],testDDL.getDatabaseName(),"YES",tenantNames[0],
				   nr,tenantNames[1],testDDL.getDatabaseName(),"NO",getIgnore()));
		// anything on the tenant now should fail
		try {
			tenantConnection.execute("show databases");
			fail("suspended tenant shouldn't be able to do squat");
		} catch (Exception e) {
			assertException(e, SQLException.class,
					"Internal error: Your account has been disabled");
		}
		rootConnection.execute("resume tenant " + tenantNames[0]);
		rootConnection.assertResults("show tenants", 
				br(nr,AdaptiveMultitenantSchemaPolicyContext.LANDLORD_TENANT,testDDL.getDatabaseName(),"NO",getIgnore(),
				   nr,tenantNames[0],testDDL.getDatabaseName(),"NO",tenantNames[0],
				   nr,tenantNames[1],testDDL.getDatabaseName(),"NO",getIgnore()));
		tenantConnection.assertResults("show databases", 
				br(nr,PEConstants.INFORMATION_SCHEMA_DBNAME,
				   nr,PEConstants.LANDLORD_TENANT,
				   nr,"mtt",
				   nr,PEConstants.MYSQL_SCHEMA_DBNAME,
				   nr,"stt"));
		// dropping database on the second tenant should remove it
		rootConnection.execute("drop database " + tenantNames[1]);
		rootConnection.assertResults("show tenants",
				br(nr,AdaptiveMultitenantSchemaPolicyContext.LANDLORD_TENANT,testDDL.getDatabaseName(),"NO",getIgnore(),
				   nr,"mtt",testDDL.getDatabaseName(),"NO",tenantNames[0]));
		// make sure that dropping a multitenant database as a regular database pukes hard
		try {
			rootConnection.execute("drop database mtdb");
			fail("should be unable to drop a multitenant database without using drop multitenant database");
		} catch (Exception e) {
			assertException(e, SQLException.class,
					"Internal error: Illegal drop database statement.  Use DROP MULTITENANT DATABASE to drop a multitenant database");
		}
	}

	private static final String block_table_decl_p1 = 
		"create table `block` ( "
		+ "`bid` int(11) not null auto_increment, "
		+ "`module` varchar(64) not null default '', "
		+ "`delta` varchar(32) not null default '0', ";
	private static final String block_table_decl_p2 = 
		"`theme` varchar(64) not null default '', "
		+ "primary key (`bid`), "
		+ "unique key `tmd` (`theme`, `module`, `delta`), "
		+ "key `list` (`theme`, `module`)) "
		+ "ENGINE=InnoDB";
	
	private static final String block_table_decl =
		block_table_decl_p1 + block_table_decl_p2;

	private static final String copy_table_decl =
		"create table `square` ( "
		+ "`squid` int(11) not null, "
		+ "`module` varchar(64) not null default '', "
		+ "primary key (`squid`))"
		+ "ENGINE=InnoDB";
	
	@Test
	public void testDDL() throws Throwable {
		setContext("testDDL");
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);
		for(int i = 0; i < tenantNames.length; i++) {
			tenantConnection.execute("use " + tenantNames[i]);
			tenantConnection.execute(block_table_decl);
		}
		// should verify that the table was correctly modified.
		CatalogDAO cat = CatalogDAOFactory.newInstance();
		// find the db with name
		UserDatabase edb = cat.findDatabase("mtdb");
		SchemaContext sc = SchemaContext.createContext(cat);
		sc.setOptions(ParserOptions.NONE);
		PEDatabase ped = sc.findPEDatabase(new UnqualifiedName("mtdb"));
		sc.setCurrentDatabase(ped);
		for(UserTable ut : edb.getUserTables()) {		
			TableInstance ti = ped.getSchema().buildInstance(sc, new UnqualifiedName(ut.getName()), null, false);
			PETable tab = ti.getAbstractTable().asTable();
			TenantColumn tc = tab.getTenantColumn(sc);
			assertNotNull(tc);
			PEKey pk = tab.getPrimaryKey(sc);
			assertNotNull(pk);
			assertEquals("pk columns", 2, pk.getKeyColumns().size()); 
			assertTrue("pk must include tenant column", pk.getKeyColumns().get(0).getColumn().isTenantColumn()); 
			cat.close();
		}
		try {
			SchemaTest.echo(" ********* MT INFO SCHEMA *********");
			CatalogQueryTest.exerciseSchema(rootConnection,"information_schema");
			CatalogQueryTest.exerciseSchema(rootConnection,"mysql");
			for(int i = 0; i < tenantNames.length; i++) {
				tenantConnection.execute("use " + tenantNames[i]);
				CatalogQueryTest.exerciseSchema(tenantConnection,"information_schema",
						"generation_site","group_provider","range_distribution","storage_generation",
						"storage_group","storage_site","site_instance","tenant","external_service","scopes");
				try {
					CatalogQueryTest.exerciseSchema(tenantConnection,"mysql","user");
				} catch (PEException e) {
					assertSchemaException(e,"No such database: mysql");
				}
			}
		} finally {
			rootConnection.execute("use mtdb");
		}
	}
		
	
	@Test
	public void testInserts() throws Throwable {
		setContext("testInserts");
		// also have to create a second user
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
	//	rootConnection.execute("alter dve set plan_cache_limit = 0;");
		createTenant(0);
		createTenant(1);
		rootConnection.execute(block_table_decl);
		rootConnection.execute("insert into `block` (`module`, `delta`, `theme`) values ('b','b','b')");
		rootConnection.assertResults("select * from block", br(nr,new Integer(1),"b","b","b"));
		rootConnection.execute(copy_table_decl);
		for(int i = 0; i < tenantNames.length; i++) {
			tenantConnection.execute("use " + tenantNames[i]);
			tenantConnection.execute(block_table_decl);
			tenantConnection.execute("insert into `block` (`module`, `delta`, `theme`) values ('a','a','a')");
			tenantConnection.assertResults("select * from block", br(nr,new Integer(1),"a","a","a"));
			tenantConnection.execute(copy_table_decl);
			tenantConnection.execute("insert into `square` (`squid`, `module`) select `bid`, `module` from `block`");
			tenantConnection.assertResults("select * from square", br(nr,new Integer(1),"a"));
			rootConnection.assertResults("select * from square",br());
		}
		becomeL();
		ResourceResponse rr = rootConnection.fetch("show tables like '%block%'");
		String tn = (String) rr.getResults().get(0).getResultColumn(1).getColumnValue();
		rootConnection.assertResults("select * from " + tn + " order by ___mtid",
				br(nr,new Integer(1),"b","b","b",getIgnore(),
				   nr,new Integer(1),"a","a","a",getIgnore(),
				   nr,new Integer(1),"a","a","a",getIgnore()));
		try {
			// make sure that if you do an insert as the null tenant, you have to specify everything
			rootConnection.execute("insert into " + tn + " (`module`, `delta`, `theme`) values ('c','c','c')");
			fail("should not be able to insert in null tenant mode if mtid not specified");
		} catch (SQLException e) {
			assertSQLException(e,
					MySQLErrors.missingDatabaseFormatter,
					"No database selected");
		}
	}

	@Test
	public void testScoping() throws Throwable {
		setContext("testScoping");
		// also have to create a second user
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("alter dve set " + VariableConstants.TABLE_GARBAGE_COLLECTOR_INTERVAL_NAME + " = 1001");
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);
		String visibilitySQL = "select table_state, scope_name from information_schema.scopes where tenant_name = '%s' order by scope_name";
		for(int i = 0; i < tenantNames.length; i++) {
			tenantConnection.execute("use " + tenantNames[i]);
			tenantConnection.execute("create table adblock (`bid` int(11) not null auto_increment, `blech` varchar(32) not null)");
			tenantConnection.execute("insert into adblock (`blech`) values ('first entry')");
			tenantConnection.assertResults("select * from adblock",br(nr,new Integer(1),"first entry"));
			// verify that the backing table exists and is shared
			rootConnection.assertResults(String.format(visibilitySQL,tenantNames[i]),
					br(nr,"SHARED","adblock"));
			try {
				tenantConnection.execute("select * from block");
				fail("tenant connection should not be able to see block");
			} catch (SQLException e) {
				assertSQLException(e,
						MySQLErrors.missingTableFormatter,
						"Table 'mtdb.block' doesn't exist");
			}
			tenantConnection.execute(block_table_decl);
			// verify that the backing table exists and is shared
			rootConnection.assertResults(String.format(visibilitySQL,tenantNames[i]),
					br(nr,"SHARED","adblock",nr,"SHARED","block"));
			tenantConnection.execute("drop table adblock");
			tenantConnection.execute("drop table block");
			// depends on table garbage collector running - ignore
			// assertExistence(actualAdBlockName,"mtdb",false);
			// assertExistence(actualBlockName,"mtdb",false);
		}
	}
	
	@Test
	public void testShow() throws Throwable {
		setContext("testShow");
		// also have to create a second user
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		becomeLT();
		createTenant(0);
		createTenant(1);
		rootConnection.execute(block_table_decl);
		rootConnection.assertResults("show tables like 'block'",br(nr,"block"));
		becomeL();
		for(int i = 0; i < tenantNames.length; i++) {
			tenantConnection.execute("use " + tenantNames[i]);
			Object[] none = br();
			// on the tenant connection the block table is not yet visible
			tenantConnection.assertResults("show tables like 'block'",none);
			// nor is it visible in any of the varieties
			tenantConnection.assertResults("show tables", none);
			tenantConnection.assertResults("show tables like '%blo%'",none);
			// now, when we create it, should become visible in all varieties
			tenantConnection.execute(block_table_decl);
			Object[] one = br(nr,"block");
			tenantConnection.assertResults("show tables like 'block'", one);
			tenantConnection.assertResults("show tables", one);
			tenantConnection.assertResults("show tables like '%blo%'",one);
			tenantConnection.execute("create table adblock (`bid` int(11) not null auto_increment, `blech` varchar(32) not null)");
			tenantConnection.assertResults("show tables like 'block'", one);
			tenantConnection.assertResults("show tables like 'adblock'",br(nr,"adblock"));
			tenantConnection.assertResults("show tables", br(nr,"block",nr,"adblock"));
			tenantConnection.assertResults("show tables like '%blo%'", br(nr,"block",nr,"adblock"));
			tenantConnection.assertResults("show tables like '%ad%'", br(nr,"adblock"));
			Object[] fullBlockCols = 
				br(nr,"bid",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore(),
						nr,"module",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore(),
						nr,"delta",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore(),
						nr,"theme",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore(),
						nr,"___mtid",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore());
			Object[] tenantBlockCols =
				br(nr,"bid",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore(),
						nr,"module",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore(),
						nr,"delta",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore(),
						nr,"theme",getIgnore(),getIgnore(),getIgnore(),getIgnore(),getIgnore());
			String tn = (String) rootConnection.fetch("show tables like '%block%'").getResults().get(0).getResultColumn(1).getColumnValue();
			rootConnection.execute("set @@dve_metadata_extensions = 1");
			rootConnection.assertResults("describe " + tn, fullBlockCols);
			rootConnection.execute("set @@dve_metadata_extensions = 0");
			tenantConnection.assertResults("describe block",tenantBlockCols);
			tenantConnection.assertResults("show columns in adblock",
					br(nr,"bid", getIgnore(), getIgnore(), getIgnore(), getIgnore(), getIgnore(),
							nr,"blech", getIgnore(), getIgnore(), getIgnore(), getIgnore(), getIgnore()));
			String ct = getCreateTable(rootConnection, tn);
			assertTrue("root connection create tbl stmt shows tenant column", ct.indexOf("___mtid") > -1);		
			ct = getCreateTable(tenantConnection,"block");
			assertTrue("tenant connection create tbl stmt does not show tenant column", ct.indexOf("___mtid") == -1);
			ct = getCreateTable(tenantConnection,"adblock");
			assertTrue("tenant connection create tbl stmt does not show tenant column", ct.indexOf("___mtid") == -1);
			// so the show commands we care about are:
			// show create table, show table status, show database, show column, show triggers
			// these all need to be scoped to the current tenant
			// tenantConnection.execute("create table adblock (`bid` int(11) not null auto_increment, `blech` varchar(32) not null)");
		}		
		
	}
	
	@Test
	public void testAlter() throws Throwable {
		setContext("testAlter");
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);
		rootConnection.execute(block_table_decl);
		for(int i = 1; i < tenantNames.length; i++) {
			tenantConnection.execute("use " + tenantNames[i]);
			tenantConnection.execute(block_table_decl);
			// tenants should not be allowed to mod global tables
			try {
				tenantConnection.execute("alter table block add `head` int not null");
			} catch (PEException e) {
				assertSchemaException(e, "You do not have permission to alter a table");
			}
			// but root can
			rootConnection.execute("alter table block add `head` int not null");
			String ct = getCreateTable(rootConnection, "block");
			assertTrue("should have new column", ct.indexOf("head") > -1);
			tenantConnection.execute("create table adblock (`bid` int(11) not null auto_increment, `blech` varchar(32) not null)");
			tenantConnection.execute("alter table adblock add `head` int not null");
			ct = getCreateTable(tenantConnection, "adblock");
			assertTrue("should have new column", ct.indexOf("head") > -1);
		}
	}
	
	private String getCreateTable(ConnectionResource conn, String tabname) throws Throwable {
		List<ResultRow> results = conn.fetch("show create table " + tabname).getResults();
		assertEquals("only one table for show create table",1,results.size());
		ResultRow rr = results.get(0);
		assertEquals("two columns for show create table",2,rr.getRow().size());
		return (String)rr.getResultColumn(2).getColumnValue();
	}
	
	// tests for the various kinds of mt support.  for each we want to test landlord tenant create, drop, alter
	// tenant create, drop, alter
	// insert into two tenants, select, update, delete
	// select by landlord

	// test for adaptive - somewhat special - ordering matters - need a third and fourth tenant
	// also, set the limit to 2 before we start
	// tenant 1 create A, populate
	// tenant 2 create A, check for flip 
	// tenant 3 create A, check that it hops on the shared table
	// verify select for all tenants
	// verify show tables for all tenants
	// verify select for landlord tenant (gets all data)
	// tenant 1 create table B
	// tenant 1 drop table A - verify still shared
	// tenant 2 create table B - different def
	// tenant 1 drop table B - tenant 2 still has B
	// tenant 2 recreate table A with different def - verify error
	// tenant 2 drop table A
	// tenant 3 drop table A - verify no A globally
	
	
	@Test
	public void testMTAdaptive() throws Throwable {
		setContext("testMTAdaptive");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		rootConnection.execute("alter dve set " + VariableConstants.TABLE_GARBAGE_COLLECTOR_INTERVAL_NAME + " = 1001");
		
		createTenant(0);
		createTenant(1);
		createAdaptiveTenant(0);
		createAdaptiveTenant(1);
		String declA = "create table tmtAA (`id` int auto_increment, `payload` varchar(32))";
		String popA = "insert into tmtAA (`payload`) values ('quick'),('brown')";
		String declB = "create table tmtBB (`id` int auto_increment, `payload` varchar(32))";
		String popB = "insert into tmtBB (`payload`) values ('quick'),('brown')";
		String declB2 = "create table tmtBB (`payload` varchar(32), `pid` int auto_increment)";
		String declAprime = "create table tmtAA (`payload` varchar(32), `sid` int auto_increment)";
		String[] tens = new String[] { tenantNames[0], tenantNames[1], adaptiveTenants[0], adaptiveTenants[1] };
		// we do lazy migration now, split out the tenant valued column so as to force the flip
		for(int i = 0; i < tens.length; i++) {
			tenantConnection.execute("use " + tens[i]);
			tenantConnection.execute(declA);
			tenantConnection.execute(popA);
			tenantConnection.assertResults("select id, payload from tmtAA order by id", 
					br(nr,new Integer(1),"quick",
					   nr,new Integer(2),"brown"));
			tenantConnection.assertResults("show tables", br(nr,"tmtAA"));			
		}
		for(int i = 0; i < tens.length; i++) {
			tenantConnection.execute("use " + tens[i]);
			tenantConnection.execute("insert into tmtAA (`payload`) values ('" + tens[i] + "')");
		}
		becomeL();
		ResourceResponse rr = rootConnection.fetch("show tables like '%tmtAA%'");
		String tabName = null;
		List<ResultRow> results = rr.getResults();
		assertEquals("should only have one table so far in adaptive mode",results.size(),1);
		tabName = (String)results.get(0).getResultColumn(1).getColumnValue();
		rootConnection.assertResults("select id, payload, ___mtid from " + tabName + " order by ___mtid, id",
				br(nr,new Integer(1),"quick",getIgnore(),
				   nr,new Integer(2),"brown",getIgnore(),
				   nr,new Integer(3),tens[0],getIgnore(),
				   nr,new Integer(1),"quick",getIgnore(),
				   nr,new Integer(2),"brown",getIgnore(),
				   nr,new Integer(3),tens[1],getIgnore(),
				   nr,new Integer(1),"quick",getIgnore(),
				   nr,new Integer(2),"brown",getIgnore(),
				   nr,new Integer(3),tens[2],getIgnore(),
				   nr,new Integer(1),"quick",getIgnore(),
				   nr,new Integer(2),"brown",getIgnore(),
				   nr,new Integer(3),tens[3],getIgnore()));
		// 
		// tenant 1 create table B
		// tenant 1 drop table A - verify still shared
		// tenant 2 create table B - different def
		// tenant 1 drop table B - tenant 2 still has B
		// tenant 2 recreate table A with different def - verify error
		// tenant 2 drop table A
		// tenant 3 drop table A - verify no A globally
		tenantConnection.execute("use " + tens[0]);
		tenantConnection.execute(declB);
		tenantConnection.execute(popB + ", ('" + tens[0] + "')");
		tenantConnection.execute("drop table tmtAA");
		tenantConnection.assertResults("show tables",br(nr,"tmtBB"));
		tenantConnection.assertResults("select id, payload from tmtBB order by id", 
				br(nr,new Integer(1),"quick",
				   nr,new Integer(2),"brown",
				   nr,new Integer(3),tens[0]));
		tenantConnection.assertResults("describe tmtBB",
				br(nr,"id","int(11)","YES","",null,"auto_increment",
				   nr,"payload","varchar(32)","YES","",null,""));
		rootConnection.assertResults("select id, payload, ___mtid from " + tabName + " order by ___mtid, id",
				br(nr,new Integer(1),"quick",getIgnore(),
				   nr,new Integer(2),"brown",getIgnore(),
				   nr,new Integer(3),tens[1],getIgnore(),
				   nr,new Integer(1),"quick",getIgnore(),
				   nr,new Integer(2),"brown",getIgnore(),
				   nr,new Integer(3),tens[2],getIgnore(),
				   nr,new Integer(1),"quick",getIgnore(),
				   nr,new Integer(2),"brown",getIgnore(),
				   nr,new Integer(3),tens[3],getIgnore()));
		for(int i = 1; i < tens.length; i++) {
			tenantConnection.execute("use " + tens[i]);
			tenantConnection.assertResults("show tables like 'tmtAA'",br(nr,"tmtAA"));
		}
		tenantConnection.execute("use " + tens[1]);
		tenantConnection.execute(declB2);
		tenantConnection.execute(popB + ", ('" + tens[1] + "')");
		tenantConnection.assertResults("select pid, payload from tmtBB order by pid", 
				br(nr,new Integer(1),"quick",
				   nr,new Integer(2),"brown",
				   nr,new Integer(3),tens[1]));
		tenantConnection.assertResults("describe tmtBB",
				br(nr,"payload","varchar(32)","YES","",null,"",
				   nr,"pid","int(11)","YES","",null,"auto_increment"));
		tenantConnection.execute("use " + tens[0]);
		tenantConnection.execute("drop table tmtBB");
		tenantConnection.assertResults("show tables",br());
		tenantConnection.execute("use " + tens[1]);
		tenantConnection.assertResults("show tables",br(nr,"tmtAA",nr,"tmtBB"));
		try {
			tenantConnection.execute(declAprime);
		} catch (Exception e) {
			assertException(e, SQLException.class,
					"Internal error: Invalid redeclaration of table: table definitions differ");
		}
		tenantConnection.execute("drop table tmtAA");
		tenantConnection.assertResults("show tables",br(nr,"tmtBB"));
		tenantConnection.execute(declAprime);
		tenantConnection.assertResults("describe tmtAA",
				br(nr,"payload","varchar(32)","YES","",null,"",
				   nr,"sid","int(11)","YES","",null,"auto_increment"));
		tenantConnection.assertResults("show tables", br(nr,"tmtAA",nr,"tmtBB"));
		tenantConnection.execute("drop table tmtAA");
		tenantConnection.execute("drop table tmtBB");
		tenantConnection.assertResults("show tables",br());
		tenantConnection.execute("use " + tens[2]);
		tenantConnection.execute("drop table tmtAA");
		tenantConnection.execute("use " + tens[3]);
		tenantConnection.execute("drop table tmtAA");
		// since tables are now dropped by the garbage collector - can't test on this anymore
		// rootConnection.assertResults("show tables",br());
	}

	
	@Test
	public void testDropTenant() throws Throwable {
		setContext("testDropTenant");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);
		String[] sts = new String[] { "st1", "st2", "st3" };
		String[] pts = new String[] { "pt1" };
		for(String stn : sts)
			rootConnection.execute("create table " + stn + " (id int, junk varchar(32))");
		for(int i = 0; i < tenantNames.length; i++) {
			tenantConnection.execute("use " + tenantNames[i]);
			// populate all three the same way
			String values = " values (1,'if'), (2,'you'), (3, 'are'), (4, 'happy'), (5, 'wag'), (6, 'your'), (7, 'tail')";
			for(String tn : sts) {
				tenantConnection.execute("create table " + tn + " (id int, junk varchar(32))");
				tenantConnection.execute("insert into " + tn + values);
			}
			for(String tn : pts) {
				tenantConnection.execute("create table " + tn + " (id int, junk varchar(32))");
				tenantConnection.execute("insert into " + tn + values);
			}
		}
		// make sure the data is all there
		becomeL();
		ResourceResponse rr = rootConnection.fetch("show tables");
		HashMap<String,String> mangledNames = new HashMap<String,String>();
		String[] ats = new String[] { "st1", "st2", "st3", "pt1" };
		for(ResultRow row : rr.getResults()) {
			String tn = (String) row.getResultColumn(1).getColumnValue();
			for(String s : ats) {
				if (tn.indexOf(s) > -1)
					mangledNames.put(s,tn);
			}
			rootConnection.assertResults("select count(*) from " + tn, br(nr,new Long(14)));
		}
		// we want to try dropping a table from a tenant, dropping a shared table from global, dropping a tenant
		// so:
		// ten1 drop st1
		// lt drop st2
		// ten0 drop pt1
		// lt drop ten1
		tenantConnection.execute("use " + tenantNames[1]);
		tenantConnection.execute("drop table st1");
		rootConnection.assertResults("select count(*) from " + mangledNames.get("st1"),br(nr,new Long(7)));
		tenantConnection.assertResults("show tables",br(nr,"st2",nr,"st3",nr,"pt1"));
		becomeLT();
		tenantConnection.execute("drop table st2");
		tenantConnection.assertResults("show tables",br(nr,"st3",nr,"pt1"));
		becomeL();
		rootConnection.execute("drop database " + tenantNames[1]);
		rootConnection.assertResults("select count(*) from " + mangledNames.get("st1"),br(nr,new Long(7)));
		rootConnection.assertResults("select count(*) from " + mangledNames.get("st3"),br(nr,new Long(7)));
	}
	
	@Test
	public void testCreateDatabaseA() throws Throwable {
		setContext("testCreateDatabaseA");
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		// create some other databases as tenants
		rootConnection.execute("use mysql");
		rootConnection.execute("create database " + tenantNames[0]);
		rootConnection.execute("create database " + tenantNames[1] + " default charset utf8");
		rootConnection.assertResults("show multitenant databases", br(nr,testDDL.getDatabaseName()));
		rootConnection.assertResults("show multitenant databases like '" + testDDL.getDatabaseName() + "'",
				br(nr,testDDL.getDatabaseName()));
		rootConnection.assertResults("show databases",br(nr,PEConstants.INFORMATION_SCHEMA_DBNAME,nr,PEConstants.LANDLORD_TENANT, nr, "mtt",nr,"mysql",nr,"stt"));
		rootConnection.assertResults("show databases like '" + tenantNames[1] + "'", br(nr,"stt"));
		rootConnection.execute("drop database " + tenantNames[1]);
		rootConnection.assertResults("show databases",br(nr,PEConstants.INFORMATION_SCHEMA_DBNAME,nr,PEConstants.LANDLORD_TENANT, nr, "mtt",nr,"mysql"));
	}
	
	@Test
	public void testCreateDatabaseB() throws Throwable {
		setContext("testCreateDatabaseB");
		String mtdbdecl = testDDL.getCreateDatabaseStatement();
		String dbname = testDDL.getDatabaseName();
		String qdbname = "`" + dbname + "`";
		String qmtdbdecl = mtdbdecl.replaceFirst(dbname, qdbname);
		rootConnection.execute(qmtdbdecl);
		becomeLT();
		rootConnection.execute("create table `dtest` (`id` int, primary key(`id`))");
		rootConnection.execute("use mysql");
		rootConnection.assertResults("show multitenant databases",br(nr,dbname));
		rootConnection.execute("create tenant " + tenantNames[0] + " on " + dbname);
		rootConnection.execute("create tenant `" + tenantNames[1] + "` on " + qdbname);
		rootConnection.assertResults("show databases like '%tt'", br(nr,"mtt",nr,"stt"));
		rootConnection.assertResults("show databases like '%foobar%'",br());
		for(int i = 0; i < tenantNames.length; i++) {
			rootConnection.execute("grant all on " + tenantNames[i] + ".* to '" + mtuserName + "'@'" + mtuserAccess + "' identified by '" + mtuserName + "'");
			tenantConnection.execute("use " + tenantNames[i]);
			tenantConnection.execute("create table `dtest` (`id` int, primary key (`id`))");
		}
		rootConnection.execute("drop database `" + tenantNames[1] + "`");
	}

	// reuse most of the tests from the alter test
	
	@Test
	public void testMTAdaptiveAltersAddChangeRemoveColumn() throws Throwable {
		setContext("testMTAdaptiveAltersAddChangeRemoveColumn");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		createTenant(1);
		createAdaptiveTenant(0);
		createAdaptiveTenant(1);
		String def1 = "create table `altest` ( `cola` int not null auto_increment, `module` varchar(64) not null, primary key (`cola`))";
		String pop1 = "insert into altest (module) values ('quick'),('scots'),('rule')";
		String[] tens = new String[] { tenantNames[0], tenantNames[1], adaptiveTenants[0], adaptiveTenants[1] };
		int ntens = tens.length;
		for(int nt = 2; nt <= tens.length; nt++) {
			ntens = nt;
			SchemaTest.echo("tenants = " + ntens);
			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute(def1);
				tenantConnection.execute(pop1 + ",('" + tens[i] + "')");
				String cts = AlterTest.getCreateTable(tenantConnection, "altest");
				assertAutoInc(tenantConnection,cts);
			}
			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute("alter table altest add `book` int not null");
				String cts = AlterTest.getCreateTable(tenantConnection, "altest");
				assertTrue("should have new column",cts.indexOf("book") > -1);
				tenantConnection.assertResults("show columns in altest like 'book'",br(nr,"book","int(11)","NO","",null,""));
				assertAutoInc(tenantConnection,cts);
			}
			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute("alter table altest change `book` `pamphlet` varchar(64) null");
				String cts = AlterTest.getCreateTable(tenantConnection,"altest");
				assertTrue("show have new col def",cts.indexOf("pamphlet") > -1);
				assertAutoInc(tenantConnection,cts);
			}

			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute("alter table altest alter column `pamphlet` set default 'howdy stranger'");
				String cts = AlterTest.getCreateTable(tenantConnection,"altest");
				assertTrue("should have def value",cts.indexOf("howdy stranger") > -1);				
				assertAutoInc(tenantConnection,cts);
			}

			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute("alter table altest alter column `pamphlet` drop default");
				String cts = AlterTest.getCreateTable(tenantConnection,"altest");
				assertEquals("should not have def value",-1,cts.indexOf("howdy stranger"));
				assertAutoInc(tenantConnection,cts);
			}

			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute("alter table altest drop `pamphlet`");
				String cts = AlterTest.getCreateTable(tenantConnection,"altest");
				assertEquals("should not have removed column",-1,cts.indexOf("pamphlet"));
				tenantConnection.assertResults("show columns in altest like 'pamphlet'", br());
				assertAutoInc(tenantConnection,cts);
			}			

			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute("alter table altest add index `indone` (`cola`,`module`)");
				String cts = AlterTest.getCreateTable(tenantConnection,"altest");
				assertTrue("should have index",cts.indexOf("indone") > -1);
				assertAutoInc(tenantConnection,cts);

			}

			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				tenantConnection.execute("alter table altest drop index `indone`");
				String cts = AlterTest.getCreateTable(tenantConnection,"altest");
				assertEquals("should not have index",-1,cts.indexOf("indone"));
				assertAutoInc(tenantConnection,cts);
			}

			for(int i = 0; i < ntens; i++) {
				tenantConnection.execute("use " + tens[i]);
				// verify that the data still exists after all this moving around
				tenantConnection.assertResults("select cola, module from altest order by cola", 
						br(nr,new Integer(1),"quick",
								nr,new Integer(2),"scots",
								nr,new Integer(3),"rule",
								nr,new Integer(4),tens[i]));
				tenantConnection.execute("drop table altest");
				tenantConnection.assertResults("show tables",br());
			}
		}
	}

    @Test
    public void testPE1037_MTAltersSkipLazyCopyForShareLimitOne() throws Throwable {
        setContext("testPE1037_MTAltersSkipLazyCopyForShareLimitOne");

        rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
        createTenant(0);
        createTenant(1);
        createAdaptiveTenant(0);
        createAdaptiveTenant(1);
        String def1 = "create table `altest` ( `cola` int not null auto_increment, `module` varchar(64) not null, primary key (`cola`))";
        String pop1 = "insert into altest (module) values ('quick'),('scots'),('rule')";
        String[] tens = new String[] { adaptiveTenants[0], adaptiveTenants[1] };
        int ntens = tens.length;

        ntens = 2;
        SchemaTest.echo("tenants = " + ntens);

        //create scoped tables and populate with tenant specific data.
        for(int i = 0; i < ntens; i++) {
            tenantConnection.execute("use " + tens[i]);
            tenantConnection.execute(def1);
            tenantConnection.execute(pop1 + ",('" + tens[i] + "')");
            String cts = AlterTest.getCreateTable(tenantConnection, "altest");
            assertAutoInc(tenantConnection,cts);
        }

        assertSharedUserTableForSameLocalTable("altest", tens);


        //have each tenant run the same alter table command, but no DML that would trigger a lazy compaction.
        for(int i = 0; i < ntens; i++) {
            tenantConnection.execute("use " + tens[i]);
            tenantConnection.execute("alter table altest add `book` int not null");
            String cts = AlterTest.getCreateTable(tenantConnection, "altest");
            assertTrue("should have new column",cts.indexOf("book") > -1);
            tenantConnection.assertResults("show columns in altest like 'book'",br(nr,"book","int(11)","NO","",null,""));
            assertAutoInc(tenantConnection,cts);
        }

        assertSharedUserTableForSameLocalTable("altest", tens);

        for(int i = 0; i < ntens; i++) {
            tenantConnection.execute("use " + tens[i]);
            tenantConnection.execute("drop table altest");
            tenantConnection.assertResults("show tables",br());
        }

    }

    private void assertSharedUserTableForSameLocalTable(String localName, String... tenantNames) throws Throwable {
        CatalogDAO catalog = CatalogDAOFactory.newInstance();
        Set<UserTable> backingTables = new HashSet<UserTable>();
        try {
            for (String tenantName : tenantNames){
                Tenant tenant = catalog.findTenant( tenantName );
                TableVisibility scope = catalog.findScope( tenant.getId(), localName );
                if (scope == null)
                    fail(String.format("Tenant %s expected to have scope for %s, didn't find one",tenantName,localName));
                UserTable userTab = scope.getTable();
                backingTables.add( userTab );
            }
            if (backingTables.size() != 1)
                fail(String.format("For local table %s, tenants %s have multiple user tables %s",localName, Arrays.asList(tenantNames),backingTables));
        } finally {
            catalog.close();
        }

    }

	@Test
	public void testPE291() throws Throwable {
		setContext("testPE291");
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);
		rootConnection.execute(block_table_decl);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute(block_table_decl);
		for(int j = 1; j <= 5; j++) {
			StringBuilder insert = new StringBuilder();
			insert.append("INSERT INTO `block` ");
			insert.append("VALUES ( ");
			insert.append(j + ",");
			insert.append("'" + j + "',");
			insert.append("'" + j + "',");
			insert.append("'" + j + "')");
			tenantConnection.execute(insert.toString());
		}
		// test that id as a literal works
		tenantConnection.assertResults("select * from `block` where `bid` in ('3')",
				br(nr,3,"3","3","3"));
	}
	
	@Test
	public void testCacheInvalidation() throws Throwable {
		setContext("testCacheInvalidation");
        rootConnection.execute(testDDL.getCreateDatabaseStatement()); 
        rootConnection.execute("use mtdb"); 
        createTenant(0); 
        tenantConnection.execute("use " + tenantNames[0]); 
        tenantConnection.execute(block_table_decl); 
        for(int j = 1; j <= 5; j++) { 
             StringBuilder insert = new StringBuilder(); 
             insert.append("INSERT INTO `block` "); 
             insert.append("VALUES ( "); 
             insert.append(j + ","); 
             insert.append("'" + j + "',"); 
             insert.append("'" + j + "',"); 
             insert.append("'" + j + "')"); 
             tenantConnection.execute(insert.toString()); 
        } 

        tenantConnection.execute("drop table `block`"); 
        tenantConnection.execute(block_table_decl); 
        for(int j = 1; j <= 5; j++) { 
             StringBuilder insert = new StringBuilder(); 
             insert.append("INSERT INTO `block` "); 
             insert.append("VALUES ( "); 
             insert.append(j + ","); 
             insert.append("'" + j + "',"); 
             insert.append("'" + j + "',"); 
             insert.append("'" + j + "')"); 
             tenantConnection.execute(insert.toString()); 
        } 
	}
	
	@Test
	public void testDataLocks() throws Throwable {
		setContext("testDataLocks");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		createTenant(1);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table foo (`id` int, `value` varchar(32), primary key (id))");
		tenantConnection.execute("use " + tenantNames[1]);
		tenantConnection.execute("create table foo (`id` int, `value` varchar(32), primary key (id))");
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("insert into foo values (1,'one'),(2,'two')");
		tenantConnection.execute("start transaction");
		tenantConnection.assertResults("select * from foo order by id",br(nr,new Integer(1),"one", nr, new Integer(2), "two"));
		rootConnection.assertResults("show multitenant lock",
                br(
                        nr,"PE.Model.Range",ignore,"acquired","SHARED","[SHARED]",1,0,"default statement generation lock", //one for root
                        nr,"PE.Model.Range",ignore,"acquired","SHARED","[SHARED]",1,0,"default statement generation lock", //one for tenant
                        nr,"PETenant:mtt/foo",ignore,"acquired","SHARED","[SHARED]",1,0,"select" //and the tenant dml lock
                )
        );
		tenantConnection.execute("update foo set value='uno' where id=1");
		tenantConnection.execute("commit");
		tenantConnection.execute("use " + tenantNames[1]);
		tenantConnection.execute("insert into foo values (1,'one'),(2,'two')");
		tenantConnection.execute("start transaction");
		tenantConnection.assertResults("select * from foo order by id",br(nr,new Integer(1),"one", nr, new Integer(2), "two"));
		rootConnection.assertResults("show multitenant lock",
                br(
                        nr,"PE.Model.Range",ignore,"acquired","SHARED","[SHARED]",1,0,"default statement generation lock", //one for root
                        nr,"PE.Model.Range",ignore,"acquired","SHARED","[SHARED]",1,0,"default statement generation lock", //one for tenant
                        nr,"PETenant:stt/foo",ignore,"acquired","SHARED","[SHARED]",1,0,"lookup scope for mt cached plan" //and the landlord
                )
        );
		tenantConnection.execute("commit");		
	}
	
	@Test
	@Ignore
	public void testSessionVariables() throws Throwable {
		setContext("testSessionVariables");
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);
		
		testMTVariable("long_query_time", "10.0", "5.5");
		testMTVariable("group_concat_max_len", "1024", "512");
	}
	
	private void testMTVariable(final String variableName, final String defaultValue, final String newValue) throws Throwable {
		assertMTVariable(0, variableName, defaultValue);
		assertMTVariable(1, variableName, defaultValue);
		
		setVariable(0, variableName, newValue);
		
		assertMTVariable(0, variableName, newValue);
		assertMTVariable(1, variableName, defaultValue);
	}
	
	private void assertMTVariable(final int tenantIndex, final String variableName, final String expectedValue) throws Throwable {
		tenantConnection.execute("use " + tenantNames[tenantIndex]);
		tenantConnection.assertResults("show variables like '" + variableName + "'", br(nr, variableName, expectedValue));
	}
	
	private void setVariable(final int tenantIndex, final String variableName, final String newValue) throws Throwable {
		tenantConnection.execute("use " + tenantNames[tenantIndex]);
		tenantConnection.execute("set " + variableName + " = " + newValue);
	}
	
	@Test
	public void testPE1163() throws Throwable {
		setContext("testPE1163");
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);
		SelectTest.testUsePerformance(tenantConnection, tenantNames);
	}

	@Test
	public void testTruncateStatement() throws Throwable {
		setContext("testTruncateStatement");
		rootConnection.execute(testDDL.getCreateDatabaseStatement());
		rootConnection.execute("use mtdb");
		createTenant(0);
		createTenant(1);

		/*
		 * Test non-AI truncate.
		 */
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table a (id INT not null)");
		tenantConnection.execute(TruncateTest.buildTableColumnInsert("a", "id", Arrays.asList(1, 2, 3)));
		tenantConnection.execute("use " + tenantNames[1]);
		tenantConnection.execute("create table a (id INT not null)");
		tenantConnection.execute(TruncateTest.buildTableColumnInsert("a", "id", Arrays.asList(1, 2, 3, 4)));

		TruncateTest.testTableTruncate(tenantConnection, tenantNames[0], "a", 3);
		TruncateTest.testTableTruncate(tenantConnection, tenantNames[1], "a", 4);

		/*
		 * Test truncate with AI columns.
		 */
		final List<Integer> b0Data = Arrays.asList(null, null, null);
		final List<Integer> b1Data = Arrays.asList(null, null, null, null);

		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table b (id INT not null auto_increment)");
		tenantConnection.execute(TruncateTest.buildTableColumnInsert("b", "id", b0Data));
		tenantConnection.execute("use " + tenantNames[1]);
		tenantConnection.execute("create table b (id INT not null auto_increment)");
		tenantConnection.execute(TruncateTest.buildTableColumnInsert("b", "id", b1Data));

		TruncateTest.testTableTruncate(tenantConnection, tenantNames[0], "b", 3);
		TruncateTest.testTableTruncate(tenantConnection, tenantNames[1], "b", 4);

		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute(TruncateTest.buildTableColumnInsert("b", "id", b0Data));
		tenantConnection.assertResults("select * from b order by id", br(nr, 1, nr, 2, nr, 3));

		tenantConnection.execute("use " + tenantNames[1]);
		tenantConnection.execute(TruncateTest.buildTableColumnInsert("b", "id", b1Data));
		tenantConnection.assertResults("select * from b order by id", br(nr, 1, nr, 2, nr, 3, nr, 4));
	}

}
