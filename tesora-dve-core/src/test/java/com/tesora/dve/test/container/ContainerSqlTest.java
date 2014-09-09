package com.tesora.dve.test.container;

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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.ContainerDistributionModel;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;

public class ContainerSqlTest extends SchemaTest {

	private static final StorageGroupDDL theStorageGroup = 
		new StorageGroupDDL("check",3,"checkg");

	private static final ProjectDDL testDDL =
		new PEDDL("checkdb",theStorageGroup,"schema");

	private static final NativeDDL nativeDDL =
			new NativeDDL("cdb");
	
	private static DBHelperConnectionResource nativeConn;
	private static TestResource nmr;
	private static int persistentGroupId;
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);

		ProxyConnectionResource pcr = new ProxyConnectionResource();
		testDDL.create(pcr);
		pcr.disconnect();
		pcr = null;

		nativeConn = new DBHelperConnectionResource();

		nmr = new TestResource(nativeConn,nativeDDL);
		nmr.create();

		nativeConn.execute("use " + TestCatalogHelper.getInstance(PETest.class).getCatalogDBName());

		ResourceResponse rr = nativeConn.execute("select persistent_group_id from persistent_group where name = '" + testDDL.getPersistentGroup().getName() + "'");
		persistentGroupId = (Integer)(rr.getResults().get(0).getResultColumn(1).getColumnValue());
	}

	@AfterClass
	public static void cleanup() throws Throwable {
		if(nmr != null)
			nmr.destroy();
		nmr = null;
		if(nativeConn != null)
			nativeConn.disconnect();
		nativeConn = null;
	}
	
	protected ProxyConnectionResource conn;
	
	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
	}
	
	@After
	public void after() throws Throwable {
		conn.disconnect();
	}

	@Test
	public void testCreateContainer() throws Throwable {
		String container1 = "cont1";
		conn.execute("CREATE CONTAINER " + container1 + " PERSISTENT GROUP " + testDDL.getPersistentGroup().getName() + " BROADCAST DISTRIBUTE");
		
		// make sure via native connection that the catalog is what we expect
		nmr.getConnection().assertResults("select name, base_table_id, storage_group_id from container where name = '" + container1 + "'", 
				br(nr,container1,null,persistentGroupId));

		conn.assertResults("SHOW CONTAINERS",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));
		conn.assertResults("SHOW CONTAINERS LIKE '%duk%'",
				br());
		conn.assertResults("SHOW CONTAINERS LIKE '%cont%'",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = 'duk'",
				br());
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = '" + container1 + "'",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));

		conn.execute("DROP CONTAINER " + container1);

		nmr.getConnection().assertResults("select name, base_table_id, storage_group_id from container where name = '" + container1 + "'", 
				br());
		
		conn.assertResults("SHOW CONTAINERS",
				br());
		conn.assertResults("SHOW CONTAINERS LIKE '%duk%'",
				br());
		conn.assertResults("SHOW CONTAINERS LIKE '%cont%'",
				br());
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = 'duk'",
				br());
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = '" + container1 + "'",
				br());
	
		conn.execute("use information_schema");
		conn.assertResults("DESCRIBE CONTAINER",
				br(nr,"CONTAINER_NAME","varchar(255)", "NO", "", null, "", 
				   nr,"BASE_TABLE","int(11)", "YES", "", null, "",
				   nr,"STORAGE_GROUP","int(11)", "NO", "", null, ""));

		// test if exists
		conn.execute("CREATE CONTAINER IF NOT EXISTS " + container1 + " PERSISTENT GROUP " + testDDL.getPersistentGroup().getName() + " BROADCAST DISTRIBUTE");

		nmr.getConnection().assertResults("select name, base_table_id, storage_group_id from container where name = '" + container1 + "'", 
				br(nr,container1,null,persistentGroupId));

		conn.assertResults("SHOW CONTAINERS",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));
		conn.assertResults("SHOW CONTAINERS LIKE '%duk%'",
				br());
		conn.assertResults("SHOW CONTAINERS LIKE '%cont%'",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = 'duk'",
				br());
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = '" + container1 + "'",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));

		conn.execute("CREATE CONTAINER IF NOT EXISTS " + container1 + " PERSISTENT GROUP " + testDDL.getPersistentGroup().getName() + " BROADCAST DISTRIBUTE");

		nmr.getConnection().assertResults("select name, base_table_id, storage_group_id from container where name = '" + container1 + "'", 
				br(nr,container1,null,persistentGroupId));

		conn.assertResults("SHOW CONTAINERS",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));
		conn.assertResults("SHOW CONTAINERS LIKE '%duk%'",
				br());
		conn.assertResults("SHOW CONTAINERS LIKE '%cont%'",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = 'duk'",
				br());
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = '" + container1 + "'",
				br(nr,"cont1",null,testDDL.getPersistentGroup().getName()));

		try {
			conn.execute("CREATE CONTAINER " + container1 + " PERSISTENT GROUP " + testDDL.getPersistentGroup().getName() + " BROADCAST DISTRIBUTE");
		} catch (Throwable e) {
			assertTrue("Expected create if not exists range to throw exception", 
					StringUtils.equals("Container " + container1 + " already exists.",
							e.getMessage()));
		}

		conn.execute("DROP CONTAINER IF EXISTS " + container1);

		nmr.getConnection().assertResults("select name, base_table_id, storage_group_id from container where name = '" + container1 + "'", 
				br());
		
		conn.assertResults("SHOW CONTAINERS",
				br());
		conn.assertResults("SHOW CONTAINERS LIKE '%duk%'",
				br());
		conn.assertResults("SHOW CONTAINERS LIKE '%cont%'",
				br());
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = 'duk'",
				br());
		conn.assertResults("SHOW CONTAINERS WHERE `Container` = '" + container1 + "'",
				br());

		try {
			conn.execute("DROP CONTAINER " + container1);
			fail("DROP non-existent container should throw exception");
		} catch (Exception e) {
			// expected
		}
		
		conn.execute("DROP CONTAINER IF EXISTS " + container1);
	}

	@Test
	public void testAddToContainer() throws Throwable {
		conn.execute("CREATE RANGE cont_range1 (int) PERSISTENT GROUP " + testDDL.getPersistentGroup().getName());
		conn.execute("CREATE CONTAINER cont1 PERSISTENT GROUP " + testDDL.getPersistentGroup().getName() + " range distribute using cont_range1");
		conn.execute("CREATE CONTAINER cont2 PERSISTENT GROUP " + testDDL.getPersistentGroup().getName() + " random distribute");
		conn.execute("CREATE RANGE cont_range2 (int, varchar(10)) PERSISTENT GROUP " + testDDL.getPersistentGroup().getName());
		
		// test if not exists for range should not throw exception here
		conn.execute("CREATE RANGE IF NOT EXISTS cont_range1 (int) PERSISTENT GROUP " + testDDL.getPersistentGroup().getName());
		try {
			conn.execute("CREATE RANGE cont_range2 (int, varchar(10)) PERSISTENT GROUP " + testDDL.getPersistentGroup().getName());
		} catch (Throwable e) {
			assertTrue("Expected create if not exists range to throw exception", 
					StringUtils.equals("Range cont_range2 already exists.",
							e.getMessage()));
		}
		
		conn.execute("use checkdb");
		
		String[] table_decls = new String[] {
				"CREATE TABLE A (id int, col2 varchar(10), primary key (id)) DISCRIMINATE ON (id) USING CONTAINER cont1",
				"CREATE TABLE A2 (id int, col2 varchar(10), primary key (id, col2)) DISCRIMINATE ON (id, col2) USING CONTAINER cont2",
				"CREATE TABLE B (id int, tbla_id int, tbleid int, primary key (id)) CONTAINER DISTRIBUTE cont1",
				"CREATE TABLE C (id int, tblb_id int, primary key (id)) CONTAINER DISTRIBUTE cont1",
				"CREATE TABLE D (dkey varchar(10), type int, primary key (dkey)) CONTAINER DISTRIBUTE cont1",
				"CREATE TABLE E (id int, col2 varchar(10), primary key (id)) PERSISTENT GROUP " + testDDL.getPersistentGroup().getName() + " BROADCAST DISTRIBUTE"				
		};

		for(String decl : table_decls) {
			conn.execute(decl);
		}
		
		// show the container member tables in the container
		conn.assertResults("SHOW CONTAINER duk",
				br());
		conn.assertResults("SHOW CONTAINER cont1",
				br(nr,"A","base",
				   nr,"B","member",
				   nr,"C","member",
				   nr,"D","member"));
	
		// TODO
		// add tests for the container vector stuff
		
		// make sure catalog is what we expect
		// check table A
		String tableName = "A";
		String containerName = "cont1";
		int tableId = getIntFromQuery("select table_id from user_table where name = '" + tableName + "'");
		int containerId = getIntFromQuery("select container_id from container where name = '" + containerName + "'");
		nativeConn.assertResults("select name, base_table_id, storage_group_id from container where name = '" + containerName + "'", 
				br(nr,containerName,tableId,persistentGroupId));
		nativeConn.assertResults("select container_id from user_table where name = '" + tableName + "'", 
				br(nr,containerId));
		
		// check table A2
		tableName = "A2";
		containerName = "cont2";
//		String columnName1 = "id";
//		String columnName2 = "col2";
		tableId = getIntFromQuery("select table_id from user_table where name = '" + tableName + "'");
//		int columnId1 = getIntFromQuery("select uc.user_column_id from user_column uc left join user_table ut on uc.user_table_id = ut.table_id where uc.name = '" + columnName1 + "' and ut.name = '" + tableName + "'");
//		int columnId2 = getIntFromQuery("select uc.user_column_id from user_column uc left join user_table ut on uc.user_table_id = ut.table_id where uc.name = '" + columnName2 + "' and ut.name = '" + tableName + "'");
		containerId = getIntFromQuery("select container_id from container where name = '" + containerName + "'");
		nativeConn.assertResults("select name, base_table_id, storage_group_id from container where name = '" + containerName + "'", 
				br(nr,containerName,tableId,persistentGroupId));
//		nativeConn.assertResults("select order_in_dist_vect, container_id, dist_vect_column_id from container_vector where container_id = " + containerId, 
//				br(nr,0,containerId,columnId1,
//				   nr,1,containerId,columnId2));
		nativeConn.assertResults("select container_id from user_table where name = '" + tableName + "'", 
				br(nr,containerId));

		// check table B
		tableName = "B";
		containerName = "cont1";
		containerId = getIntFromQuery("select container_id from container where name = '" + containerName + "'");
		nativeConn.assertResults("select container_id from user_table where name = '" + tableName + "'", 
				br(nr,containerId));

		// check table C
		tableName = "C";
		containerName = "cont1";
		containerId = getIntFromQuery("select container_id from container where name = '" + containerName + "'");
		nativeConn.assertResults("select container_id from user_table where name = '" + tableName + "'", 
				br(nr,containerId));

		// check table D
		tableName = "D";
		containerName = "cont1";
		containerId = getIntFromQuery("select container_id from container where name = '" + containerName + "'");
		nativeConn.assertResults("select container_id from user_table where name = '" + tableName + "'", 
				br(nr,containerId));

		// check table E
		tableName = "E";
		nativeConn.assertResults("select container_id from user_table where name = '" + tableName + "'", 
				br(nr,null));
	}

	@Test
	public void testTemplate() throws Throwable {
		String dbName = testDDL.getDatabaseName();
		String templateName = "container_test";
		String containerName = "cont_test";
		String baseTableName = "A";
		String memberTableName = "B";
		String[] discriminatorCols = new String[] {"id1", "id2"}; 
		
		try {
			conn.execute("drop database if exists " + dbName);
			// set the template
			conn.execute(new TemplateBuilder(templateName)
				.withRequirement("create range if not exists cont_range (int) persistent group #sg#")
				.withRequirement("create container if not exists " + containerName + " persistent group #sg# range distribute using cont_range")
				.withContainerTable(baseTableName, containerName, "id1","id2")
				.withContainerTable(memberTableName, containerName)
				.toCreateStatement());
	
			conn.execute("create database " + dbName + " default persistent group " + testDDL.getPersistentGroup().getName() + " using template " + templateName + " strict");
			
			CatalogDAO cat = CatalogDAOFactory.newInstance();
			UserDatabase udb = cat.findDatabase(dbName);
			assertEquals("UserDatabase has template name",templateName,udb.getTemplateName());
			assertTrue("UserDatabase has strict template", udb.hasStrictTemplateMode());
			assertNotNull("Range should exist",cat.findRangeByName("cont_range", testDDL.getPersistentGroup().getName(), false));
			cat.close();
	
			conn.execute("use " + dbName);
			conn.execute("create table " + baseTableName + " (id1 int, id2 int, col2 varchar(10), primary key (id1, id2))");
			conn.execute("create table " + memberTableName + " (id int, col2 varchar(10), primary key (id))");
			
			cat = CatalogDAOFactory.newInstance();
			udb = cat.findDatabase(dbName);
			
			UserTable ut = udb.getTableByName(baseTableName);
			assertEquals("Table " + baseTableName + " should have container distribution",ContainerDistributionModel.MODEL_NAME,ut.getDistributionModel().getName());
			assertTrue("Table " + baseTableName + " should be a container base table",ut.isContainerBaseTable());
			assertNotNull("Table " + baseTableName + " is a container member table",ut.getContainer());
			for (int i = 0; i < discriminatorCols.length; i++) {
				String col = discriminatorCols[i];
				UserColumn uc = ut.getUserColumn(col);
				assertNotNull("Column " + col + " must exist", uc);
				assertTrue("Column " + col + " must be position " + (i+1), 
						uc.getCDV_Position() == (i+1));
			}

			ut = udb.getTableByName(memberTableName);
			assertEquals("Table " + memberTableName + " should have container distribution",ContainerDistributionModel.MODEL_NAME,ut.getDistributionModel().getName());
			assertFalse("Table " + memberTableName + " is NOT a container base table",ut.isContainerBaseTable());
			assertNotNull("Table " + memberTableName + " is a container member table",ut.getContainer());

			cat.close();
		} finally {
			conn.execute("drop database " + dbName);
		}
	}
		
	private Integer getIntFromQuery(String query) throws Throwable {
		ResourceResponse rr = nativeConn.execute(query);
		return (Integer)(rr.getResults().get(0).getResultColumn(1).getColumnValue());
	}
}
