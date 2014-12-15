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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.tesora.dve.charset.NativeCollationCatalog;
import com.tesora.dve.charset.NativeCollationCatalogImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.db.mysql.common.ColumnAttributes;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.standalone.DefaultClassLoad;
import com.tesora.dve.standalone.PETest;

public class CatalogTest extends PETest {

	static {
		logger = Logger.getLogger(CatalogTest.class);
	}

	static Class<?> bootClass = PETest.class;
	static Properties testProps = new Properties();

	@BeforeClass
	public static void setup() throws PEException, SQLException {
		TestCatalogHelper.createMinimalCatalog(bootClass);
		TestHost.startServices(PETest.class);
		testProps = PEFileUtils.loadPropertiesFile(DefaultClassLoad.class, PEConstants.CONFIG_FILE_NAME);
	}
	
	@After
	public void runAfter() {
		TestHost.stopServices();
	}

	@Test
	public void testUserTableAndColumn() throws PEException {
		DistributionModel dm = new BroadcastDistributionModel();
		PersistentGroup sg1 = new PersistentGroup("Default");
		UserDatabase udb = new UserDatabase(UserDatabase.DEFAULT, sg1);

		UserTable ut1 = new UserTable("foo", dm, udb, TableState.SHARED, PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);

		ColumnSet cs1 = new ColumnSet();

		cs1.addColumn(new ColumnMetadata("col1", ColumnAttributes.SIZED_TYPE, 1, "CHAR"));
		cs1.addColumn(new ColumnMetadata("col2", ColumnAttributes.SIZED_TYPE, 4, "INTEGER"));
		cs1.addColumn(new ColumnMetadata("col3", ColumnAttributes.SIZED_TYPE, 12, "VARCHAR"));

		ut1.addColumnMetadataList(cs1.getColumnList());

		// check that some of the attributes match after the add
		assertTrue(cs1.getColumn(1).getName()
				.equals(ut1.getUserColumn("col1").getName()));
		assertTrue(cs1.getColumn(1).getDataType() == ut1.getUserColumn("col1")
				.getDataType());
		assertTrue(cs1.getColumn(1).getTypeName()
				.equals(ut1.getUserColumn("col1").getTypeName()));

		assertTrue(cs1.getColumn(2).getName()
				.equals(ut1.getUserColumn("col2").getName()));
		assertTrue(cs1.getColumn(2).getDataType() == ut1.getUserColumn("col2")
				.getDataType());
		assertTrue(cs1.getColumn(2).getTypeName()
				.equals(ut1.getUserColumn("col2").getTypeName()));

		assertTrue(cs1.getColumn(3).getName()
				.equals(ut1.getUserColumn("col3").getName()));
		assertTrue(cs1.getColumn(3).getDataType() == ut1.getUserColumn("col3")
				.getDataType());
		assertTrue(cs1.getColumn(3).getTypeName()
				.equals(ut1.getUserColumn("col3").getTypeName()));

	}

	@Test
	public void testCreateCatalogEntities() throws Exception {
		CatalogDAO c = CatalogDAOFactory.newInstance(testProps);

		try {
			Collection<DistributionModel> dists = c.findAllDistributionModels();
			assertFalse(dists.isEmpty());

			logger.info("Output from testCreateDistributionModel():");

			for (DistributionModel dist : dists) {
				logger.info(dist);
			}

			// testCreateStorageGroup
			c.begin();
			PersistentGroup sg1 = c.createPersistentGroup("Default");
			PersistentSite db1 = c.createPersistentSite("db1", "jdbc:mysql://localhost/db1", "root", "password");
			sg1.addStorageSite(db1);
			PersistentSite db2 = c.createPersistentSite("db2", "jdbc:mysql://localhost/db2", "root", "password");
			sg1.addStorageSite(db2);
			c.createPersistentGroup("My Persistent Group");
			c.commit();

			Collection<PersistentGroup> sgs = c.findAllPersistentGroups();
			assertFalse(sgs.isEmpty());

			logger.info("Output from testCreateStorageGroup():");

			for (PersistentGroup sg : sgs) {
				logger.info(sg);
			}

			// testCreateProject
			c.begin();
			c.createProject("MyProject");
			sg1 = c.findDefaultPersistentGroup();
			assertNotNull(sg1);
			c.commit();
			c.begin();
            c.createDatabase("MyCatalog", sg1, Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet(),
					Singletons.require(HostService.class).getDBNative().getDefaultServerCollation());
			c.commit();

			// testCreateUserTable
			Map<String, DistributionModel> distMap = c
					.getDistributionModelMap();
			PersistentGroup defSG = c.findDefaultPersistentGroup();
			PersistentGroup mySG = c.findPersistentGroup("My Persistent Group");
			
			UserDatabase userDB = c.findDatabase("MyCatalog");

			c.begin();
			c.createUserTable(userDB, "foo",
					distMap.get(BroadcastDistributionModel.MODEL_NAME),
					defSG,
					PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
			c.createUserTable(userDB, "bar",
					distMap.get(RandomDistributionModel.MODEL_NAME),
					mySG,
					PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
			c.createUserTable(userDB, "foobar",
					distMap.get(StaticDistributionModel.MODEL_NAME),
					defSG,
					PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
			c.createUserTable(userDB, "barfoo",
					distMap.get(BroadcastDistributionModel.MODEL_NAME),
					defSG,
					PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
			c.commit();

			Collection<UserTable> uts = c.findAllUserTables();
			assertFalse(uts.isEmpty());

			logger.info("Output from testCreateUserTable():");

			for (UserTable ut : uts) {
				logger.info(ut);
			}

			/*
			DistributionModel dist_bcast = c
					.findDistributionModel(BroadcastDistributionModel.MODEL_NAME);

			Collection<UserTable> utsInBcast = dist_bcast.getUserTables();
			logger.info("Tables in " + dist_bcast.getName() + "("
					+ utsInBcast.size() + ")");

			assertTrue(dist_bcast.getUserTables().size() > 0);
			assertFalse(utsInBcast.isEmpty());
			for (UserTable ut : utsInBcast) {
				logger.info(ut);
			}
			 */

			PersistentGroup sg_def = c.findDefaultPersistentGroup();
			logger.info("Tables in " + sg_def.getName());

			Collection<UserTable> utsInDef = c.findAllTablesInPersistentGroup(sg_def);
			assertFalse(utsInDef.isEmpty());
			for (UserTable ut : utsInDef) {
				logger.info(ut);
			}

			// testCreateUserColumn
			UserTable ut3 = (UserTable) c
					.createQuery("from UserTable where name='foobar'")
					.getResultList().get(0);
			assertNotNull(ut3);

			c.begin();
			c.createUserColumn(ut3, "col1", 1, "char", 10);
			c.createUserColumn(ut3, "col2", 1, "char", 10);
			c.createUserColumn(ut3, "col3", 1, "char", 10);
			c.createUserColumn(ut3, "col4", 1, "char", 10);
			c.createUserColumn(ut3, "col5", 1, "char", 10);
			c.commit();

			c.refresh(ut3);
			
			Collection<UserColumn> ucs = ut3.getUserColumns();
			assertFalse(ucs.isEmpty());

			logger.info("Output from testCreateUserColumns():");

			for (UserColumn uc : ucs) {
				logger.info(uc);
			}
		} finally {
			c.close();
		}
		return;
	}
	
	@Test
	public void testAddUser() throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(testProps);
			
		try {
			c.begin();
			assertNotNull(c.createUser("foo", "bar", "myhost"));
			c.commit();

			List<User> users = c.findUsers("foo", "myhost");
			assertTrue("should have only one matching user",users.size() == 1);
			User u = users.get(0);
			assertEquals("foo", u.getName());
			assertEquals("bar", u.getPlaintextPassword());
			assertEquals("myhost", u.getAccessSpec());
			assertFalse(u.getAdminUser());

			c.begin();
			assertNotNull(c.createUser("adminfoo", "bar", "myhost", true));
			c.commit();

			users = c.findUsers("adminfoo","myhost");
			assertTrue("should have only one matching user",users.size() == 1);
			u = users.get(0);
			assertEquals("adminfoo", u.getName());
			assertEquals("bar", u.getPlaintextPassword());
			assertEquals("myhost", u.getAccessSpec());
			assertTrue(u.getAdminUser());

		
		} finally {
			c.close();
		}
	}

	@Test
	public void testAddProvider() throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(testProps);
			
		try {
			c.begin();
			assertNotNull(c.createProvider("foo", "com.tesora.dve.provider.foo"));
			c.commit();

			Provider pr = c.findProvider("foo");
			assertEquals("foo", pr.getName());
			assertEquals("com.tesora.dve.provider.foo", pr.getPlugin());
			assertTrue(pr.isEnabled());
			
			c.begin();
			Provider pr2 = c.findProvider("foo");
			pr2.setIsEnabled(false);
			c.commit();
			
			c.refresh(pr);
			assertFalse(pr.isEnabled());
			
		} finally {
			c.close();
		}
	}
	
	@Test
	public void testAddExternalService() throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(testProps);
		
		try {
			c.begin();
			assertNotNull(c.createExternalService("foo", "com.tesora.dve.provider.foo", "root", true ));
			c.commit();

			ExternalService es = c.findExternalService("foo");
			assertEquals("foo", es.getName());
			assertEquals("com.tesora.dve.provider.foo", es.getPlugin());
			assertTrue(es.isAutoStart());
			assertTrue(es.usesDataStore());
			assertEquals("root", es.getConnectUser());
		} finally {
			c.close();
		}
	}

	@Test
	public void testCharacterSets() throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(testProps);
		
		try {
			// test defaults
			List<CharacterSets> charSets = c.findAllCharacterSets();
			assertEquals(4, charSets.size());
			
			for(CharacterSets cs : charSets) {
				if(StringUtils.equals(cs.getCharacterSetName(), "ascii")) {
					assertTrue(StringUtils.equals(Singletons.require(NativeCollationCatalog.class).findDefaultCollationForCharSet(cs.getCharacterSetName()).getName(), "ascii_general_ci"));
					assertTrue(StringUtils.equals(cs.getDescription(), "US ASCII"));
					assertTrue(StringUtils.equals(cs.getPeCharacterSetName(), "US-ASCII"));
				} else if (StringUtils.equals(cs.getCharacterSetName(), "latin1")) {
					assertTrue(StringUtils.equals(Singletons.require(NativeCollationCatalog.class).findDefaultCollationForCharSet(cs.getCharacterSetName()).getName(), "latin1_swedish_ci"));
					assertTrue(StringUtils.equals(cs.getDescription(), "cp1252 West European"));
					assertTrue(StringUtils.equals(cs.getPeCharacterSetName(), "ISO-8859-1"));
				} else if(StringUtils.equals(cs.getCharacterSetName(), "utf8")) {
					assertTrue(StringUtils.equals(Singletons.require(NativeCollationCatalog.class).findDefaultCollationForCharSet(cs.getCharacterSetName()).getName(), "utf8_general_ci"));
					assertTrue(StringUtils.equals(cs.getDescription(), "UTF-8 Unicode"));
					assertTrue(StringUtils.equals(cs.getPeCharacterSetName(), "UTF-8"));
				} else if(StringUtils.equals(cs.getCharacterSetName(), "utf8mb4")) {
					assertTrue(StringUtils.equals(Singletons.require(NativeCollationCatalog.class).findDefaultCollationForCharSet(cs.getCharacterSetName()).getName(), "utf8mb4_general_ci"));
					assertTrue(StringUtils.equals(cs.getDescription(), "UTF-8 Unicode"));
					assertTrue(StringUtils.equals(cs.getPeCharacterSetName(), "UTF-8"));
				} else {
					fail("Invalid default character set: " + cs.getCharacterSetName());
				}
			}
		} finally {
			c.close();
		}
	}
}
