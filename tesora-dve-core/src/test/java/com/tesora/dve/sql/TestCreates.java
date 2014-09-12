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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlNativeTypeCatalog;
import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ProxyConnectionResourceResponse;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class TestCreates extends SchemaTest {
	private static final StorageGroupDDL sgDDL = new StorageGroupDDL("mttsg",1,"pg");
	private static final PEDDL testDDL =
		new PEDDL("mtdb", 
				sgDDL,
				"database");
	private static final PEDDL testDDL_FK_MODE_IGNORE =
		new PEDDL("mtdbfkmodeignore", 
				sgDDL,
				"database").withFKMode(FKMode.IGNORE);
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(testDDL, testDDL_FK_MODE_IGNORE);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource rc = new ProxyConnectionResource();
		testDDL.create(rc);
		testDDL_FK_MODE_IGNORE.create(rc);
		rc.disconnect();
	}	

	ProxyConnectionResource rootConnection;
	ProxyConnectionResource rootConnection_fk_mode_ignore;
	
	@Before
	public void before() throws Throwable {
		rootConnection = new ProxyConnectionResource();
		rootConnection.execute("use " + testDDL.getDatabaseName());
		rootConnection_fk_mode_ignore = new ProxyConnectionResource();
		rootConnection_fk_mode_ignore.execute("use " + testDDL_FK_MODE_IGNORE.getDatabaseName());
	}
	
	@After
	public void after() throws Throwable {
		rootConnection.disconnect();
		rootConnection_fk_mode_ignore.disconnect();
	}
	
	@Test
	public void testColumnCollation() throws Throwable {
		rootConnection.execute("create table testA (`excluded` mediumtext COLLATE utf8_unicode_ci)");
		String cts = AlterTest.getCreateTable(rootConnection, "testA");
		assertTrue("collate clause should appear in create table stmt",cts.indexOf("COLLATE") > -1);
	}
	
	@Test
	public void testTableCollation() throws Throwable {
		rootConnection.execute("create table testTabCol (`id` int, `email` mediumtext) engine=myisam default charset=utf8 collate=utf8_unicode_ci");
		String cts = AlterTest.getCreateTable(rootConnection,"testTabCol");
		assertTrue("collate clause should appear in create table stmt", cts.indexOf("COLLATE") > -1 && cts.indexOf("utf8_unicode_ci") > -1);		
	}
	
	@Test
	public void testEnum() throws Throwable {
		rootConnection.execute("create table testB (`id` int, `funding` enum ('angel','bridge','A', 'B') not null default 'angel')");
		String cts = AlterTest.getCreateTable(rootConnection, "testB");
		assertTrue("enum should appear in create table stmt",cts.indexOf("bridge") > -1);
		rootConnection.assertResults("show columns in testB like 'fund%'",br(nr,"funding","enum('angel','bridge','A','B')","NO","","angel",""));
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("create table uncreatable (`id` int, `funding` enum ('bankrupt')) static distribute on (`funding`)");
			}
		}.assertError(SchemaException.class, MySQLErrors.internalFormatter,
					"Internal error: Invalid distribution column type: enum('bankrupt')");
	}
	
	@Test
	public void testColumnCharsetDecl() throws Throwable {
		rootConnection.execute("create table testD (`id` int, `product_sku` varchar(50) CHARACTER SET big5 DEFAULT NULL)");
		String cts = AlterTest.getCreateTable(rootConnection, "testD");
		assertTrue("char set should appear in create table statement",cts.indexOf("CHARACTER SET big5") > -1);
	}
	
	@Test
	public void testBizarreColumnName() throws Throwable {
		rootConnection.execute("create table bizarre (`whatevs` varchar(32),\n`  id_w_ubs` char(15) DEFAULT NULL)");
		rootConnection.assertResults("show columns in bizarre like '%id%'",
				br(nr,"  id_w_ubs","char(15)", "YES", "", null, ""));
		rootConnection.execute("insert into bizarre values ('foo', 'bar')");
		rootConnection.assertResults("select * from bizarre",br(nr,"foo","bar"));
		rootConnection.execute("create table `monet` (`\tredem_discount_point` char(15) DEFAULT NULL\n )");
		rootConnection.assertResults("show columns in monet like '%point%'",
				br(nr, "\tredem_discount_point", "char(15)", "YES", "", null, ""));
		rootConnection.execute("create table `who-are-these-people` (`id` int)");
		rootConnection.assertResults("describe `who-are-these-people`",
				br(nr,"id","int(11)","YES","",null,""));
	}
	
	@Test
	public void testOnUpdateTimestampColumn() throws Throwable {
		rootConnection.execute("create table `bs_credit`(`id` int not null, `date` timestamp not null default current_timestamp on update current_timestamp)");
		String cts = AlterTest.getCreateTable(rootConnection, "bs_credit");
		assertTrue("on update clause should be create table statement (for now)",cts.indexOf("ON UPDATE CURRENT_TIMESTAMP") > -1);
	}
	
	@Test
	public void testPreserveMyISAM() throws Throwable {
		rootConnection.execute("create table ettest (`id` int, `hescores` varchar(32)) engine = MyISAM");
		String cts = AlterTest.getCreateTable(rootConnection, "ettest");
		assertTrue("engine type should be myisam",cts.indexOf("MyISAM") > -1);
		rootConnection.assertResults("select engine from information_schema.tables where table_name = 'ettest' and table_schema = '" + testDDL.getDatabaseName() + "'",
				br(nr,"MyISAM"));		
	}

	@Test
	public void testStorageEngines() throws Throwable {
		testCreateWithStorageEngine("MyISAM");
		testCreateWithStorageEngine("InnoDB");
		testCreateWithStorageEngine("MEMORY");
		testCreateWithStorageEngine("ARCHIVE");
		testCreateWithStorageEngine("CSV");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				testCreateWithStorageEngine("BLACKHOLE");
			}
		}.assertException(PEException.class, "Invalid value for 'storage_engine' (allowed values are INNODB, MEMORY, MYISAM, ARCHIVE, CSV)");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				testCreateWithStorageEngine("FEDERATED");
			}
		}.assertException(PEException.class, "Invalid value for 'storage_engine' (allowed values are INNODB, MEMORY, MYISAM, ARCHIVE, CSV)");
	}

	private void testCreateWithStorageEngine(final String engine) throws Throwable {
		final String baseTableName = "storage_engine_test_" + engine;

		rootConnection.execute("create table " + baseTableName + "_A (`id` int not null, `hescores` varchar(32) not null) ENGINE=" + engine);
		rootConnection.execute("set storage_engine=" + engine);
		rootConnection.execute("create table " + baseTableName + "_B (`id` int not null, `hescores` varchar(32) not null)");

		assertStorageEngineForTable(engine, baseTableName + "_A");
		assertStorageEngineForTable(engine, baseTableName + "_B");
	}

	private void assertStorageEngineForTable(final String engine, final String tableName) throws Throwable {
		final String cts = AlterTest.getCreateTable(rootConnection, tableName);
		assertTrue("Table '" + tableName + "' should have '" + engine + "' engine in definition: " + cts, cts.indexOf(engine) > -1);
		rootConnection.assertResults(
				"select engine from information_schema.tables where table_name = '" + tableName + "' and table_schema = '" + testDDL.getDatabaseName() + "'",
				br(nr, engine));
	}

	@Test
	public void testFulltextIndex() throws Throwable {
		rootConnection.execute("create table ftitest (`id` int, `partay` text, `partee` text, fulltext index `dict` (`partay`, `partee`)) engine=myisam");
		rootConnection.assertResults("show keys in ftitest",
				br(nr,"ftitest",I_ONE,"dict",new Integer(1),"partay",null,getIgnore(),null,null,"YES","FULLTEXT","","",
				   nr,"ftitest",I_ONE,"dict",new Integer(2),"partee",null,getIgnore(),null,null,"YES","FULLTEXT","",""));
	}
	
	@Test
	public void testPE150() throws Throwable {
		String sql = "CREATE TABLE `accounts` (" 
				+ "`id` binary(36) NOT NULL,"
				+ "`identifier` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT 'an identifier used in integration, for example, in account it may be the Salesforce.com id',"
				+ "`enabled` tinyint(1) DEFAULT '1' COMMENT 'indicates whether the account (and its projects) can be used in the system currently',"
				+ "`atype` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'regular' COMMENT 'lookup from vela_account_type valid value',"
				+ "`activation_date` datetime DEFAULT NULL COMMENT 'the date the account was activated',"
				+ "`deactivation_date` datetime DEFAULT NULL COMMENT 'the date the account was deactivated',"
				+ "`company_id` binary(36) DEFAULT NULL COMMENT 'the account''s company',"
				+ "`created_at` datetime DEFAULT NULL,"
				+ "`updated_at` datetime DEFAULT NULL,"
				+ "`premium_account` tinyint(1) DEFAULT '0' COMMENT 'indicates this is a premium account',"
				+ "`project_capacity` int(11) DEFAULT '5' COMMENT 'Capacity, in Gigabytes of storage allocated to each project',"
				+ "`sso_key` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,"
				+ "`expiration_date` datetime DEFAULT NULL,"
				+ "`designation` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'standard',"
				+ "`salesforce_identifier` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,"
				+ "`bim_enabled` tinyint(1) DEFAULT '0',"
				+ "PRIMARY KEY (`id`),"
				+ "KEY `fk_account_company_id` (`company_id`)"
				+ "/*  CONSTRAINT `fk_account_company_id` FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`) ON DELETE SET NULL */"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Vela customers, owns users and projects';";
		rootConnection.execute(sql);
		String cts = AlterTest.getCreateTable(rootConnection, "accounts");
		assertTrue("collate clause should appear in create table stmt",cts.indexOf("COLLATE") > -1);
	}
	
	@Test
	public void testMultiDBCreate() throws Throwable {
		ProxyConnectionResourceResponse rr = (ProxyConnectionResourceResponse) rootConnection.execute(testDDL.getCreateDatabaseStatement());
		assertEquals("dup create should have 0 rows affected",rr.getNumRowsAffected(),0);

		// dup db should throw
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("create database mtdb default persistent group pg using template " + TemplateMode.OPTIONAL);
			}
		}.assertError(SchemaException.class, MySQLErrors.internalFormatter,
					"Internal error: Database mtdb already exists");
	}

	@Test
	public void testCreateDatabaseWithCharacterSetAndCollation()
			throws Throwable {
        final String defaultCharSet = Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet();
        final String defaultCollation = Singletons.require(HostService.class).getDBNative().getDefaultServerCollation();
        final NativeCharSetCatalog supportedCharsets = Singletons.require(HostService.class).getDBNative().getSupportedCharSets();

		final NativeCharSet utf8 = supportedCharsets.findCharSetByName("UTF8", true);
		final NativeCharSet ascii = supportedCharsets.findCharSetByName("ASCII", true);
		final NativeCharSet latin1 = supportedCharsets.findCharSetByName("LATIN1", true);
		final NativeCharSet utf8mb4 = supportedCharsets.findCharSetByName("UTF8MB4", true);
		
		String db = "onlypg";
		StringBuilder createDBSql = new StringBuilder();
		createDBSql.append("create database ");
		createDBSql.append(db);
		createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
		executeCreateDB(createDBSql.toString(), db, defaultCharSet,
				defaultCollation);

		db = "charsetonly";
		createDBSql = new StringBuilder();
		createDBSql.append("create database ");
		createDBSql.append(db);
		createDBSql.append(" character set = '" + utf8.getName() + "' ");
		createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
		executeCreateDB(createDBSql.toString(), db, utf8.getName(), Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(utf8.getName(), true).getName());

		db = "collationonly";
		createDBSql = new StringBuilder();
		createDBSql.append("create database ");
		createDBSql.append(db);
		createDBSql.append(" collate " + Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(latin1.getName(), true).getName());
		createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
		executeCreateDB(createDBSql.toString(), db, latin1.getName(), Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(latin1.getName(), true).getName());

		db = "charsetandcollation";
		createDBSql = new StringBuilder();
		createDBSql.append("create database ");
		createDBSql.append(db);
		createDBSql.append(" default character set '" + ascii.getName() + "' ");
		createDBSql.append(" default collate = " + Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(ascii.getName(), true).getName());
		createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
		executeCreateDB(createDBSql.toString(), db, ascii.getName(), Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(ascii.getName(), true).getName());

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				final String db = "incompatiblecharsetandcollation";
				final StringBuilder createDBSql = new StringBuilder();
				createDBSql.append("create database ");
				createDBSql.append(db);
				createDBSql.append(" default character set '" + utf8.getName() + "' ");
				createDBSql.append(" default collate = " + Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(latin1.getName(), true).getName());
				createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
				executeCreateDB(createDBSql.toString(), db, utf8.getName(), Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(latin1.getName(), true).getName());
			}
		}.assertException(SchemaException.class, "COLLATION 'latin1_swedish_ci' is not valid for CHARACTER SET 'utf8'");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				final String db = "unsupportedcharset";
				final StringBuilder createDBSql = new StringBuilder();
				createDBSql.append("create database ");
				createDBSql.append(db);
				createDBSql.append(" default character set 'big5' ");
				createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
				executeCreateDB(createDBSql.toString(), db, "big5", "big5_chinese_ci");
			}
		}.assertException(SchemaException.class, "No collations found for character set 'big5'");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				final String db = "unsupportedcollation";
				final StringBuilder createDBSql = new StringBuilder();
				createDBSql.append("create database ");
				createDBSql.append(db);
				createDBSql.append(" default collate = utf8_junk_ci");
				createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
				executeCreateDB(createDBSql.toString(), db, utf8.getName(), "utf8_junk_ci");
			}
		}.assertException(SchemaException.class, "Unsupported COLLATION 'utf8_junk_ci'");
		
		db = "UTF8MB4_charset";
		createDBSql = new StringBuilder();
		createDBSql.append("create database ");
		createDBSql.append(db);
		createDBSql.append(" default character set '" + utf8mb4.getName() + "' ");
		createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
		executeCreateDB(createDBSql.toString(), db, utf8mb4.getName(), Singletons.require(HostService.class).getDBNative().getSupportedCollations().findDefaultCollationForCharSet(utf8mb4.getName(), true).getName());
	}

	private void executeCreateDB(String createSql, String db,
			String expectedCharSet, String expectedCollation) throws Throwable {
		rootConnection.execute(createSql);
		try {
			String verifySql = "select schema_name, default_character_set_name, default_collation_name "
					+ "from information_schema.schemata where schema_name = '"
					+ db + "'";
			rootConnection.assertResults(verifySql,
					br(nr, db, expectedCharSet, expectedCollation));
		} finally {
			try {
				rootConnection.execute("drop database " + db);
			} catch (Exception e) {
				// don't worry about this
			}
		}
	}
	
	@Test
	public void testPE409() throws Throwable {
		rootConnection.execute("create table pe409 (`id` int, `sid` int, primary key (`id`)) /*! engine=innodb */ /*#dve random distribute */");
		rootConnection.execute("insert into pe409 values (1,1), (2,2), (3,3)");
		rootConnection.assertResults("show tables like '%409%'",br(nr,"pe409"));
	}
	
	@Test
	public void testIndexPrefixes() throws Throwable {
		rootConnection.execute("create table colpref (`id` int, `body` longtext, unique key (`body` (200))) engine=innodb");
		String cts = AlterTest.getCreateTable(rootConnection, "colpref");
		assertTrue("should have prefix",cts.indexOf("`body`(200)") > -1);
		rootConnection.assertResults("show keys in colpref",
				br(nr,"colpref",I_ZERO,"body",I_ONE,"body","A",getIgnore(),new Integer(200),null,"YES","BTREE","",""));
	}

	@Test
	public void testPE149()	throws Throwable {
		String db = "dbPE149";
		try {
			StringBuilder createDBSql = new StringBuilder();
			createDBSql.append("create database ");
			createDBSql.append(db);
			createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
	
			rootConnection.execute(createDBSql.toString());
	
			rootConnection.assertResults("select schema_name from information_schema.schemata where schema_name = '" + db + "'",
					br(nr, db));
	
			try {
				createDBSql = new StringBuilder();
				createDBSql.append("create database ");
				createDBSql.append(db);
				createDBSql.append(" default persistent group pg using template " + TemplateMode.OPTIONAL);
		
				rootConnection.execute(createDBSql.toString());

				fail("Should have thrown exception creating duplicate database");
			} catch (Exception e) {
				// expected
			}
	
			createDBSql = new StringBuilder();
			createDBSql.append("create database if not exists ");
			createDBSql.append(db);
			createDBSql.append(" default persistent group pg");
	
			rootConnection.execute(createDBSql.toString());

			rootConnection.assertResults("select schema_name from information_schema.schemata where schema_name = '" + db + "'",
					br(nr, db));
		} finally {
			rootConnection.execute("drop database if exists " + db);
		}
	}
	
	@Test
	public void testPE322() throws Throwable {
		rootConnection.execute("create table pe322 (`id` int not null, primary key (`id`)) engine=imaginary");
		assertTrue("should use innodb",AlterTest.getCreateTable(rootConnection, "pe322").indexOf(PEConstants.DEFAULT_DB_ENGINE) > -1);
		rootConnection.assertResults("select engine from information_schema.tables where table_name = 'pe322' and table_schema = '" + testDDL.getDatabaseName() + "'",
				br(nr,"InnoDB"));		
	}

	/**
	 * The two "bool" datatypes need to be changed to "tinyint(1)" once PE-854 is fixed
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testPE323() throws Throwable {
		rootConnection.execute("create table pe323 (`id` int, flag boolean, flag2 bool, flag3 bit)");
		rootConnection.assertResults("show columns from pe323", br(nr, "id", "int(11)", "YES", "", null, "",
				nr, "flag", "bool", "YES", "", null, "", nr, "flag2", "bool", "YES", "", null, "",
				nr, "flag3", "bit(1)", "YES", "", null, ""));
	}

	@Test
	public void testPE339() throws Throwable {
		rootConnection.execute("create table pe339 (a int, unique (a), b int not null, unique(b), c int not null, index(c))");
		rootConnection.assertResults("show keys in pe339",
				br(nr,"pe339",I_ZERO,"a",I_ONE,"a","A",getIgnore(),null,null,"YES","BTREE","","",
				   nr,"pe339",I_ZERO,"b",I_ONE,"b","A",getIgnore(),null,null,"","BTREE","","",
				   nr,"pe339",I_ONE,"c",I_ONE,"c","A",getIgnore(),null,null,"","BTREE","",""));
	}
	
	@Test
	public void testPE329() throws Throwable {
		rootConnection.execute("create table pe329 (a int not null, b int not null, primary key using BTREE (a))");
		rootConnection.assertResults("show keys in pe329",
				br(nr,"pe329",new Integer(0),"PRIMARY",new Integer(1),"a","A",getIgnore(),null, null, "", "BTREE", "", ""));
	}
	
	@Test
	public void testPE330() throws Throwable {
		// not viable on 5.1
		if (SchemaTest.getDBVersion() != DBVersion.MYSQL_51) {
			rootConnection.execute("create table pe330 (c1 varchar(10) not null, index i1 (c1) comment 'c1')");
			assertTrue("still has comment decl",AlterTest.getCreateTable(rootConnection,"pe330").indexOf("COMMENT 'c1'") > -1);
			rootConnection.assertResults("show keys in pe330",
					br(nr,"pe330",I_ONE,"i1",I_ONE,"c1","A",getIgnore(),null,null,"","BTREE","","c1"));
		}
	}
	
	@Test
	public void testMemoryEngine() throws Throwable {
		rootConnection.execute("create table tmemeng (`id` int, `other` varchar(32), primary key (`id`)) engine=memory");
		rootConnection.assertResults("select engine from information_schema.tables where table_schema = '" + testDDL.getDatabaseName() + "' and table_name = 'tmemeng'",
				br(nr,"MEMORY"));
		rootConnection.assertResults("select model_type from information_schema.distributions where database_name = '" + testDDL.getDatabaseName() + "' and table_name = 'tmemeng'",
				br(nr,"Random"));
	}

	@Test
	public void testPE382() throws Throwable {
		try {
			// simple copy of the columns
			rootConnection.execute("create table pe382 (id int not null, data int not null, primary key (id)) broadcast distribute");
			rootConnection.execute("create table copyOfpe382 like pe382");
			ResourceResponse resp = rootConnection.fetch("describe pe382");
			ResourceResponse respCopy = rootConnection.fetch("describe copyOfpe382");
			resp.assertResultsEqualUnordered("testPE382 simple describe", resp.getResults(), respCopy.getResults(), resp.getColumnCheckers());
			resp = rootConnection.fetch("show columns from pe382");
			respCopy = rootConnection.fetch("show columns from copyOfpe382");
			resp.assertResultsEqualUnordered("testPE382 simple show columns", resp.getResults(), respCopy.getResults(), resp.getColumnCheckers());

			// make sure the if not exists is a no-op
			rootConnection.execute("create table if not exists copyOfpe382 like pe382");
			resp = rootConnection.fetch("describe pe382");
			respCopy = rootConnection.fetch("describe copyOfpe382");
			resp.assertResultsEqualUnordered("testPE382 simple describe", resp.getResults(), respCopy.getResults(), resp.getColumnCheckers());
			resp = rootConnection.fetch("show columns from pe382");
			respCopy = rootConnection.fetch("show columns from copyOfpe382");
			resp.assertResultsEqualUnordered("testPE382 simple show columns", resp.getResults(), respCopy.getResults(), resp.getColumnCheckers());
			
			// fks are not supposed to be copied but indexes are copied
			rootConnection.execute("create table pe382fk (idfk int not null, aid int not null, uid int not null, primary key (idfk), unique index (uid)) broadcast distribute");
			rootConnection.execute("alter table pe382fk add foreign key (aid) references pe382(id)");
			rootConnection.execute("create table copyOfpe382fk like pe382fk");
			rootConnection.assertResults("SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE information_schema.TABLE_CONSTRAINTS.CONSTRAINT_TYPE = 'FOREIGN KEY' AND information_schema.TABLE_CONSTRAINTS.TABLE_NAME = 'pe382fk'",
					br(nr,"def","mtdb","pe382fk_ibfk_1","mtdb","pe382fk","FOREIGN KEY"));
			rootConnection.assertResults("SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE information_schema.TABLE_CONSTRAINTS.CONSTRAINT_TYPE = 'FOREIGN KEY' AND information_schema.TABLE_CONSTRAINTS.TABLE_NAME = 'copyOfpe382fk'",
					br());

			// make sure the if not exists is a no-op
			rootConnection.execute("create table if not exists copyOfpe382fk like pe382fk");
			rootConnection.assertResults("SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE information_schema.TABLE_CONSTRAINTS.CONSTRAINT_TYPE = 'FOREIGN KEY' AND information_schema.TABLE_CONSTRAINTS.TABLE_NAME = 'pe382fk'",
					br(nr,"def","mtdb","pe382fk_ibfk_1","mtdb","pe382fk","FOREIGN KEY"));
			rootConnection.assertResults("SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE information_schema.TABLE_CONSTRAINTS.CONSTRAINT_TYPE = 'FOREIGN KEY' AND information_schema.TABLE_CONSTRAINTS.TABLE_NAME = 'copyOfpe382fk'",
					br());
		} finally {
			rootConnection.execute("drop table if exists pe382fk");
			rootConnection.execute("drop table if exists copyOfpe382fk");
			rootConnection.execute("drop table if exists pe382");
			rootConnection.execute("drop table if exists copyOfpe382");
		}
	}
	
	@Test
	public void testPE653() throws Throwable {
		rootConnection.execute("create table pe653a(`id` int, primary key (`id`)) engine=innodb row_format=dynamic");
		rootConnection.assertResults("select engine, row_format from information_schema.tables where table_schema = '" + testDDL.getDatabaseName() + "' and table_name = 'pe653a'",
				br(nr,"InnoDB","DYNAMIC"));
		rootConnection.assertResults("select model_type from information_schema.distributions where database_name = '" + testDDL.getDatabaseName() + "' and table_name = 'pe653a'",
				br(nr,"Random"));
		rootConnection.execute("alter table pe653a row_format=fixed");
		rootConnection.assertResults("select engine, row_format from information_schema.tables where table_schema = '" + testDDL.getDatabaseName() + "' and table_name = 'pe653a'",
				br(nr,"InnoDB","FIXED"));
		rootConnection.assertResults("select model_type from information_schema.distributions where database_name = '" + testDDL.getDatabaseName() + "' and table_name = 'pe653a'",
				br(nr,"Random"));
		

	}
	
	@Test
	public void testPE331() throws Throwable {
		rootConnection.execute("CREATE TABLE pe331 (longdesc text DEFAULT '' NOT NULL)");

		rootConnection.execute("DROP TABLE pe331");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("CREATE TABLE pe331 (longdesc text DEFAULT ' ' NOT NULL)");
			}
		}.assertException(PESQLException.class, "(1101: 42000) BLOB/TEXT column 'longdesc' can't have a default value", true);
	}
	
	@Test
	public void testPE214() throws Throwable {
		rootConnection.execute("create table pe214a (a int primary key, b int unique key, c int key)");
		rootConnection.assertResults("show keys in pe214a",
				br(nr,"pe214a",I_ZERO,"b",I_ONE,"b","A",getIgnore(),null,null,"YES","BTREE","","",
				   nr,"pe214a",I_ONE,"c",I_ONE,"c","A",getIgnore(),null,null,"YES","BTREE","","",
				   nr,"pe214a",I_ZERO,"PRIMARY",I_ONE,"a","A",getIgnore(),null,null,"","BTREE","",""));
		rootConnection.execute("create table pe214b (a int unique key primary key)");
		rootConnection.assertResults("show keys in pe214b",
				br(nr,"pe214b",I_ZERO,"a",I_ONE,"a","A",getIgnore(),null,null,"","BTREE","","",
				   nr,"pe214b",I_ZERO,"PRIMARY",I_ONE,"a","A",getIgnore(),null,null,"","BTREE","",""));
	}
	
	@Test
	public void testPE742() throws Throwable {
		rootConnection
				.execute("create table pe742_t1 (c1 VARCHAR(10) NOT NULL COMMENT 'c1 comment', c2 INTEGER,c3 INTEGER COMMENT '012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER, INDEX i1 (c1),INDEX i2(c2)) COMMENT='abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcd';");
	}
	
	@Test
	public void testPE763() throws Throwable {
		rootConnection.execute("CREATE TABLE pe763(c1 INT UNSIGNED NOT NULL, c2 INT SIGNED NULL, c3 INT, INDEX idx2(c2))");
	}

	@Test
	public void testPE774() throws Throwable {
		rootConnection.execute("create table pe774a (`id` int, `Id2` int, ID3 int, primary key (`ID`, id2, `iD3`), unique key(ID2))");
		rootConnection.execute("create table pe774b (`id` int, primary key (`ID`), `Id2` int, ID3 int, unique key (`ID`,`id2`,`id3`))");
		rootConnection.execute("create table pe774c (`id` int, `Id2` int, primary key (`ID`, id2), ID3 int, unique key (ID3))");
		rootConnection.execute("select `id` from pe774a where ID = 1 or `ID`=2");
		rootConnection.execute("select `id` from pe774a where pe774a.Id = 1 or pe774a.`Id`=2");
		rootConnection.execute("select `id` from pe774a where `pe774a`.iD = 1 or `pe774a`.`iD`=2");
	}
	
	@Test
	public void testPE769() throws Throwable {
		rootConnection.execute("create table pe774 (i int, j int, k char(25) charset utf8)");
		rootConnection.assertResults("select character_set_name from information_schema.columns where table_name = 'pe774' and column_name = 'k'",
				br(nr,"utf8"));
	}
	
	@Test
	public void testPE842() throws Throwable {
		String[] tbls = {
				"CREATE TABLE `ira_aa` ( "
				+ "`id` varchar(32) NOT NULL, "
				+ "`qid` varchar(32) NOT NULL, "
				+ "PRIMARY KEY (`id`), "
				+ "KEY `fk_aaqi` (`qid`), "
				+ "CONSTRAINT `fk_aaqi` FOREIGN KEY (`qid`) REFERENCES `ira_aq` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION "
				+ ") ENGINE=InnoDB DEFAULT CHARSET=latin1 "
				,
				"CREATE TABLE `ira_aq` ( "
				+ "`id` varchar(32) NOT NULL, "
				+ "`tid` varchar(45) NOT NULL, "
				+ "`cai` varchar(32) DEFAULT NULL, "
				+ "PRIMARY KEY (`id`), "
				+ "KEY `fk_qt` (`tid`), "
				+ "KEY `fk_qca` (`cai`), "
				+ "CONSTRAINT `fk_qca` FOREIGN KEY "
				+ "(`cai`) REFERENCES `ira_aa` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, "
				+ "CONSTRAINT `fk_qt` FOREIGN KEY (`tid`) REFERENCES `irp_t` (`tid`) ON DELETE NO ACTION ON UPDATE NO ACTION "
				+ ") ENGINE=InnoDB DEFAULT CHARSET=latin1 "
				,
				"CREATE TABLE `ira_qi` ( "
				+ "`id` varchar(32) NOT NULL, "
				+ "`qid` varchar(32) NOT NULL, "
				+ "`tii` varchar(32) DEFAULT NULL, "
				+ "PRIMARY KEY (`id`), "
				+ "KEY `fk_qiq` (`qid`), "
				+ "KEY `fk_qiti` (`tii`), "
				+ "CONSTRAINT `fk_qiq` FOREIGN KEY (`qid`) REFERENCES `ira_aq` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, "
				+ "CONSTRAINT `fk_qiti` FOREIGN KEY (`tii`) REFERENCES `ira_t` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION "
				+ ") ENGINE=InnoDB DEFAULT CHARSET=latin1 "
				};

		rootConnection.execute("CREATE RANGE id_range (varchar(32)) PERSISTENT GROUP "
				+ testDDL_FK_MODE_IGNORE.getPersistentGroup().getName());

		String db = "PE842";
		int[] fkChecks = { 0, 1 };
		FKMode[] fkmodes = { FKMode.IGNORE, FKMode.STRICT };
		String[] dists = { "broadcast distribute", "random distribute" };
		ProxyConnectionResource rc = null;
		for (int fkCheck : fkChecks) {
			for (FKMode fkmode : fkmodes) {
				rc = rootConnection;
				if (fkmode == FKMode.IGNORE) {
					rc = rootConnection_fk_mode_ignore;
				}
				rc.execute("DROP DATABASE IF EXISTS " + db);
				rc.execute("CREATE DATABASE " + db + " DEFAULT PERSISTENT GROUP " + sgDDL.getName() + " USING TEMPLATE "
						+ TemplateMode.OPTIONAL + " FOREIGN KEY MODE " + fkmode);
				try {
					rc.execute("SET FOREIGN_KEY_CHECKS = " + fkCheck);
					rc.execute("USE " + db);
					for (String dist : dists) {
						for (String tbl : tbls) {
							try {
								String d = dist;
								if (StringUtils.startsWith(dist, "range")) {
									if (StringUtils.startsWith(tbl, "CREATE TABLE `ira_aq`")) {
										d = StringUtils.replace(dist, "%", "id");
									} else {
										d = StringUtils.replace(dist, "%", "qid");
									}
								}
								rc.execute(tbl + d);
							} catch (Exception e) {
								// do nothing will validate after loop ends
							}
						}
						
						assertTestPE842VariationResult(rc, db, fkCheck, fkmode, dist);
						
						rc.execute("drop table if exists ira_aa, ira_qi, ira_aq");
						rc.assertResults("show tables like 'ira_%'", br());
						rc.assertResults(
								"select * from information_schema.table_constraints where constraint_type='FOREIGN KEY' and table_name like 'ira_%' order by constraint_name",
								br()
								);
					}
				} finally {
					rc.execute("DROP DATABASE IF EXISTS " + db);
				}
			}
		}

		rootConnection.execute("DROP DATABASE IF EXISTS " + db);
		rootConnection.execute("CREATE DATABASE " + db + " DEFAULT PERSISTENT GROUP " + sgDDL.getName() + " USING TEMPLATE "
				+ TemplateMode.OPTIONAL);

		try {
			rootConnection.execute("SET FOREIGN_KEY_CHECKS=0");
			rootConnection.execute("USE " + db);

			rootConnection.execute("CREATE TABLE IF NOT EXISTS `Apps` ("
					+ "`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
					+ "`AppTypes_id` int(10) unsigned DEFAULT NULL,"
					+ "PRIMARY KEY (`id`),"
					+ "KEY `fk_Apps_AppTypes1` (`AppTypes_id`),"
					+ "CONSTRAINT `fk_Apps_AppTypes1` FOREIGN KEY (`AppTypes_id`)"
					+ "REFERENCES `AppTypes` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION"
					+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 broadcast distribute");
			rootConnection.execute("CREATE TABLE IF NOT EXISTS `AppTypes` ("
					+ "`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
					+ "`Apps_id` int(10) unsigned DEFAULT NULL,"
					+ "PRIMARY KEY (`id`),"
					+ "CONSTRAINT `fk_AppTypes_Apps` FOREIGN KEY (`Apps_id`)"
					+ "REFERENCES `Apps` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION"
					+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 broadcast distribute");
			rootConnection.assertResults("show tables like 'App%'",
					br(nr, "Apps", nr, "AppTypes"));
			rootConnection.assertResults("select * from Apps",
					br());
		} finally {
			rootConnection.execute("DROP DATABASE IF EXISTS " + db);
		}
	}
	
	/**
	 * CHECK INDEX  FK MODE (4) FK CHECK (2) DIST (1) # TABLES
	 * 0 			S 			0 			 B 	   	  3
	 * 1 			S 			0 			 R 	      2
	 * 2 			S 			1 			 B 	      0
	 * 3 			S 			1 			 R 	      0
	 * 4 			I			0			 B	      3
	 * 5 			I 			0 			 R 	      3
	 * 6 			I 			1 			 B 	      0
	 * 7 			I 			1 			 R 	      0
	 */
	private void assertTestPE842VariationResult(final ConnectionResource rc, final String dbName, final int fkCheck, final FKMode fkMode, final String distributionModel)
			throws Throwable {
		final int modeCheckValue = (fkMode == FKMode.STRICT) ? 0 : 4;
		final int fkCheckCheckValue = (fkCheck == 0) ? 0 : 2;
		final int distributionCheckValue = (distributionModel.startsWith("broadcast")) ? 0 : 1;
		final int checkIndex = modeCheckValue + fkCheckCheckValue + distributionCheckValue;

		switch (checkIndex) {
		case 2:
		case 3:
		case 6:
		case 7:
			rc.assertResults("show tables like 'ira_%'", br());
			rc.assertResults(
					"select * from information_schema.table_constraints where constraint_type='FOREIGN KEY' and table_name like 'ira_%' order by constraint_name",
					br());
			break;
		case 1:
			rc.assertResults("show tables like 'ira_%'", br(nr, "ira_aa", nr, "ira_qi"));
			rc.assertResults(
					"select * from information_schema.table_constraints where constraint_type='FOREIGN KEY' and table_name like 'ira_%' order by constraint_name",
					br(nr, "def", dbName, "fk_aaqi", dbName, "ira_aa", "FOREIGN KEY",
							nr, "def", dbName, "fk_qiq", dbName, "ira_qi", "FOREIGN KEY",
							nr, "def", dbName, "fk_qiti", dbName, "ira_qi", "FOREIGN KEY")
					);
			break;
		case 0:
		case 4:
		case 5:
			rc.assertResults(
					"show tables like 'ira_%'",
					br(nr, "ira_aa", nr, "ira_aq", nr,
							"ira_qi"));
			rc.assertResults(
					"select * from information_schema.table_constraints where constraint_type='FOREIGN KEY' and table_name like 'ira_%' order by constraint_name",
					br(nr, "def", dbName, "fk_aaqi", dbName, "ira_aa", "FOREIGN KEY",
							nr, "def", dbName, "fk_qca", dbName, "ira_aq", "FOREIGN KEY",
							nr, "def", dbName, "fk_qiq", dbName, "ira_qi", "FOREIGN KEY",
							nr, "def", dbName, "fk_qiti", dbName, "ira_qi", "FOREIGN KEY",
							nr, "def", dbName, "fk_qt", dbName, "ira_aq", "FOREIGN KEY")
					);
			break;
		default:
			throw new PECodingException("Unexpected test result encountered.");
		}
	}

	@Test
	public void testPE842_2() throws Throwable {
		rootConnection.execute("SET FOREIGN_KEY_CHECKS=0");
		rootConnection.execute("CREATE TABLE `irpc_s` ( " +
				  "`chi` varchar(40) NOT NULL DEFAULT '', " +
				  "`pid` varchar(40) DEFAULT NULL, " +
				  "`cl` int(11) NOT NULL, " +
				  "`sd` varchar(2048) NOT NULL, " +
				  "`code` varchar(32) NOT NULL, " +
				  "`cs` int(11) NOT NULL, " +
				  "`si` varchar(2) NOT NULL, " +
				  "`iln` tinyint(4) NOT NULL DEFAULT '0', " +
				  "`sgli` varchar(32) NOT NULL, " +
				  "`ot1` varchar(255) DEFAULT NULL, " +
				  "`sui` varchar(32) NOT NULL, " +
				  "`my` varchar(6) DEFAULT NULL, " +
				  "`mlt` varchar(1024) DEFAULT NULL, " +
				  "`st` varchar(32) DEFAULT NULL, " +
				  "`lmd` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
				  "PRIMARY KEY (`chi`), " +
				  "KEY `fkey_ss` (`si`), " +
				  "KEY `fkey_ssu` (`sui`), " +
				  "KEY `fkey_ssgl` (`sgli`), " +
				  "CONSTRAINT `fkey_ss` FOREIGN KEY (`si`) REFERENCES `ca_state` (`si`) ON DELETE NO ACTION ON UPDATE NO ACTION, " +
				  "CONSTRAINT `fkey_ssgl` FOREIGN KEY (`sgli`) REFERENCES `irpc_sgl` (`sgli`) ON DELETE NO ACTION ON UPDATE NO ACTION, " +
				  "CONSTRAINT `fkey_ssu` FOREIGN KEY (`sui`) REFERENCES `ca_is` (`sui`) ON DELETE NO ACTION ON UPDATE NO ACTION " +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1 /*#dve broadcast distribute */ ");

		rootConnection.execute("CREATE TABLE `irpc_cm` ( " +
				  "`cmi` varchar(32) NOT NULL, " +
				  "`si` varchar(2) NOT NULL, " +
				  "`ssi` varchar(40) NOT NULL, " +
				  "`ci` varchar(2) NOT NULL, " +
				  "`csi` varchar(40) NOT NULL, " +
				  "`sui` varchar(32) NOT NULL, " +
				  "`lmd` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
				  "PRIMARY KEY (`cmi`), " +
				  "KEY `fkey_ssm` (`si`,`ssi`), " +
				  "KEY `fkey_csm` (`ci`,`csi`), " +
				  "CONSTRAINT `fkey_csm` FOREIGN KEY (`ci`, `csi`) REFERENCES `irpc_s` (`si`, `chi`) ON DELETE NO ACTION ON UPDATE NO ACTION, " +
				  "CONSTRAINT `fkey_ssm` FOREIGN KEY (`si`, `ssi`) REFERENCES `irpc_s` (`si`, `chi`) ON DELETE NO ACTION ON UPDATE NO ACTION " +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1 /*#dve broadcast distribute */ ");

		rootConnection.assertResults("show tables like 'irpc_%'",
				br(nr,"irpc_cm",nr,"irpc_s"));
	}
	
	@Test
	public void testPE845() throws Throwable {
		rootConnection.execute("/*!50001 CREATE TABLE `pe845` (" 
				+"`District ID` tinyint NOT NULL,`District Name` tinyint NOT NULL,`School ID` tinyint NOT NULL," 
				+"`School Name` tinyint NOT NULL,`State` tinyint NOT NULL) ENGINE=MyISAM */");
		rootConnection.assertResults("show columns in pe845 like 'District%'",
				br(nr,"District ID","tinyint(4)","NO","",null,"",
				   nr,"District Name","tinyint(4)","NO","",null,""));
	}
	
	@Test
	public void testPE844() throws Throwable {
		rootConnection.execute("CREATE TABLE pe844 (" 
				+"`order_number` varchar(32) DEFAULT NULL,`product_line_id` varchar(32) DEFAULT NULL," 
				+"`math` bit(1) DEFAULT b'1',`creation_date` timestamp NULL DEFAULT NULL," 		
				+"KEY `FK_PRODUCT_LINE_ID` (`product_line_id`)" 
				+") ENGINE=InnoDB DEFAULT CHARSET=latin1 ");
		rootConnection.assertResults("show columns in pe844 like 'math'",
				br(nr,"math","bit(1)","YES","","b'1'",""));
	}

	/**
	 * This is the start of a test to validate all the supported MySQL types
	 * The two "bool" datatypes in testPE323 need to be changed to "tinyint(1)" once this is fixed
	 * 
	 * @throws Throwable
	 */
	@Ignore
	@Test
	public void testPE854() throws Throwable {
		StringBuilder sb = new StringBuilder("CREATE TABLE pe854 (\n");
		String colPrefix = "c";
		MysqlNativeTypeCatalog mntCatalog = new MysqlNativeTypeCatalog();
		mntCatalog.load();
		Set<Map.Entry<String, NativeType>> catEntries = mntCatalog.getTypeCatalogEntries();
		int colCount = 0;
		for (Entry<String, NativeType> entry : catEntries) {
			String type = entry.getKey();
			NativeType nativeType = entry.getValue();
			if ( nativeType.isUsedInCreate()) {
				//boolean requiresPrecision = nativeType.getSupportsPrecision();
				colCount++;
				if (colCount > 1) {
					sb.append(", ");
				}
				sb.append(colPrefix).append(colCount).append(" ").append(type).append("\n");
			}
		}
		sb.append(")");
		rootConnection.execute(sb.toString());
	}

	@Test
	public void testPE870() throws Throwable {
		rootConnection.execute("CREATE TABLE function (`function` int NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1 ");
	}

	@Test
	public void testPE959And968() throws Throwable {
		String tn = "pe959and968";
		rootConnection.execute("create table " + tn + " (id int, fid int, primary key(id)) engine=myisam max_rows=10000000000 checksum=1 comment 'ugh'");
		assertTrue("should have options in cts",AlterTest.getCreateTable(rootConnection, tn).indexOf("MAX_ROWS=4294967295 CHECKSUM=1") > -1);
		rootConnection.assertResults("select create_options from information_schema.tables where table_schema = '" + testDDL.getDatabaseName() + "' and table_name = '" + tn + "'",
				br(nr,"max_rows=4294967295 checksum=1"));
	}
	
	// test that we accurately parse and ignore partition info
	@Test
	public void testPartitionByRange() throws Throwable {
		rootConnection.execute("create table pbyrangea (id int not null, fname varchar(30), store_id int not null, primary key (id)) " 
				+"partition by range (store_id) ( "
				+"partition p0 values less than (6), "
				+"partition p1 values less than (11), "
				+"partition p2 values less than maxvalue)");
		rootConnection.execute("create table pbyrangeb (id int not null, hired date not null default '1980-01-01', primary key(id)) "
				+"partition by range (year(hired)) ( "
				+"partition p0 values less than (1991), "
				+"partition p1 values less than (1996), "
				+"partition p2 values less than maxvalue)");
		rootConnection.execute("create table pbyrangec (id int not null, last_known timestamp not null default current_timestamp on update current_timestamp, primary key(id)) "
				+"partition by range ( unix_timestamp(last_known) ) ( "
				+"partition p0 values less than ( unix_timestamp('2008-01-01 00:00:00') ), "
				+"partition p1 values less than ( unix_timestamp('2010-01-01 00:00:00') ), "
				+"partition p2 values less than (maxvalue))");
		rootConnection.execute("create table pbyranged (id int not null, joined date not null, primary key(id)) "
				+"partition by range columns(joined) ( "
				+"partition p0 values less than ('1960-01-01'),"
				+"partition p1 values less than ('1970-01-01'),"
				+"partition p2 values less than maxvalue)");
	}
	
	@Test
	public void testPartitionByList() throws Throwable {
		rootConnection.execute("create table pbylista (id int not null, pid int, primary key (id)) "
				+"partition by list (pid) ( "
				+"partition one values in (1,11,21,31,41), "
				+"partition two values in (2,12,22,32,42), "
				+"partition three values in (3,13,23,33,43) )");
	}
	
	@Test
	public void testPartitionByColumns() throws Throwable {
		rootConnection.execute("create table pbyrangecolumnsa (a int, b int, c char(3), d int, primary key(a)) "
				+"partition by range columns(a,d,c) ( "
				+"  partition p0 values less than (5,10,'ggg'),"
				+"  partition p1 values less than (10,20,'mmm'), "
				+"  partition p3 values less than (maxvalue,maxvalue,maxvalue))");
		rootConnection.execute("create table pbylistcolumnsa (id int not null, pid int, primary key(id)) "
				+"partition by list columns (pid) ( "
				+"  partition odds values in (1,3,5,7,9),"
				+"  partition evens values in (2,4,6,8,10))");
	}
	
	@Test
	public void testPartitionByHash() throws Throwable {
		rootConnection.execute("create table pbyhasha (id int not null, pid int, primary key (id)) "
				+" partition by hash (pid) partitions 4");
		rootConnection.execute("create table pbyhashb (id int not null, pid int, primary key (id)) "
				+" partition by hash (pid)");
		rootConnection.execute("create table pbyhashc (id int not null, hired date not null default '1980-01-01', primary key(id)) "
				+"partition by hash (year(hired)) partitions 4");
	}
	
	@Test
	public void testPartitionByKey() throws Throwable {
		rootConnection.execute("create table pbykeya (id int not null, name varchar(20), primary key (id)) "
				+"partition by key() partitions 2");
		rootConnection.execute("create table pbykeyb (id int not null, s1 char(32), primary key (id)) "
				+"partition by key (id) partitions 10");
		rootConnection.execute("create table pbykeyc (id int not null, col2 char(5), primary key (id)) "
				+"partition by linear key (id) partitions 3");
	}
	
	@Test
	public void testCreateRangeTable() throws Throwable {
		try {
		rootConnection.execute(
				"CREATE TEMPLATE pe432_template XML='"
						+ "<template name=\"pe432_template\">"
						+ "<tabletemplate match=\"pe432\" model=\"Range\" range=\"pe432_range\">"
						+ "<column>id</column>"
						+ "</tabletemplate>"
						+ "</template>' MATCH='pe432_db'"
		);
		rootConnection.execute("drop database if exists pe432_db");
		rootConnection.execute("create database if not exists pe432_db default persistent group pg");
		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("create table pe432_db.pe432 (id int not null)");
			}
		}.assertException(SchemaException.class,
				"com.tesora.dve.sql.SchemaException: No such range from template 'pe432_template' on storage group pg: pe432_range");
		} finally {
			try {
				rootConnection.execute("drop database if exists pe432_db");
			} catch (final Throwable t) {
				// Ignore
			}
		}
	}
	
	@Test
	public void testPE264() throws Throwable {
		rootConnection.execute("CREATE TABLE pe264 (b1 BOOLEAN, b2 BOOL, i1 INTEGER, i2 INT, d1 DECIMAL, d2 DEC, d3 FIXED, v1 CHARACTER VARYING(255), v2 VARCHAR(255), c1 CHARACTER, c2 CHAR)");
	}
	
	@Test
	public void testPE348() throws Throwable {
		rootConnection.execute("CREATE TABLE pe348 (p Point NOT NULL, ls LineString NOT NULL, pg Polygon NOT NULL, mp MultiPoint NOT NULL, mls MultiLineString NOT NULL, mpg MultiPolygon NOT NULL, g Geometry NOT NULL, gc GeometryCollection NOT NULL)");
	}
	
	@Test
	public void testPE359() throws Throwable {
		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("create table pe359 (n int not null, key(n)) delay_key_write = 1");
			}
		}.assertException(SchemaException.class, "No support for DELAY_KEY_WRITE table option");
	}
	
	@Test
	public void testPE743() throws Throwable {
		rootConnection.execute("CREATE TABLE pe743 (a ENUM(0xE4, '1', '2') not null default 0xE4, b SET(0xE4, '1', '2') not null default 0xE4)");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("CREATE TABLE pe743 (a ENUM(0xE4, '1', '2') not null default 5)");
			}
		}.assertException(SchemaException.class, "No value at position 5 in the ENUM");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("CREATE TABLE pe743 (a SET(0xE4, '1', '2') not null default 5)");
			}
		}.assertException(SchemaException.class, "No value at position 5 in the SET");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				rootConnection.execute("CREATE TABLE pe743 (a ENUM(0xE4, '1', '2') not null default '3')");
			}
		}.assertException(PESQLException.class, "(1067: 42000) Invalid default value for 'a'", true);
	}

	@Test
	public void testPE1483() throws Throwable {
		rootConnection.execute("CREATE TABLE pe1483 (`a` enum('2','x') NOT NULL DEFAULT 'x', `b` set('2','x') NOT NULL DEFAULT 'x', `c` enum('2','x') NOT NULL DEFAULT '2', `d` set('2','x') NOT NULL DEFAULT '2', `e` enum('2','x') NOT NULL DEFAULT 2, `f` set('2','x') NOT NULL DEFAULT 2)");
		rootConnection.execute("INSERT INTO pe1483 VALUES ()");
		rootConnection.assertResults("SELECT * FROM pe1483", br(nr, "x", "x", "2", "2", "x", "x"));
	}
	
	@Test
	public void testDVE1496() throws Throwable {
		rootConnection.execute("CREATE TABLE dve1496 (`a`  varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL)");
		rootConnection.assertResults("SHOW CREATE TABLE dve1496",
				br(nr, "dve1496", "CREATE TABLE `dve1496` (\n  `a` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  RANDOM DISTRIBUTE */"));
		rootConnection.execute("alter table dve1496 change `a` `a` varchar(255) character set utf8mb4 collate utf8mb4_unicode_ci DEFAULT NULL");
	}

	@Test
	public void testPE735_PE736_PE737() throws Throwable {
		rootConnection.execute("CREATE TABLE `pe73567_t0` (b BIT DEFAULT 1, i INT DEFAULT NULL);");
		rootConnection
				.assertResults(
						"show create table pe73567_t0",
						br(nr, "pe73567_t0",
								"CREATE TABLE `pe73567_t0` (\n  `b` bit(1) DEFAULT b'1',\n  `i` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  RANDOM DISTRIBUTE */"));

		rootConnection.execute("CREATE TABLE `pe73567_t1` (`a` int(11) DEFAULT '10' )");
		rootConnection.assertResults(
				"show create table pe73567_t1",
				br(nr, "pe73567_t1",
						"CREATE TABLE `pe73567_t1` (\n  `a` int(11) DEFAULT '10'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  RANDOM DISTRIBUTE */"));

		rootConnection.execute("CREATE TABLE `pe73567_t2` (`a` int(11) DEFAULT 10 )");
		rootConnection.assertResults(
				"show create table pe73567_t2",
				br(nr, "pe73567_t2",
						"CREATE TABLE `pe73567_t2` (\n  `a` int(11) DEFAULT '10'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  RANDOM DISTRIBUTE */"));

		rootConnection.execute("CREATE TABLE `pe73567_t3` (`a` double DEFAULT 10.0 )");
		rootConnection.assertResults(
				"show create table pe73567_t3",
				br(nr, "pe73567_t3",
						"CREATE TABLE `pe73567_t3` (\n  `a` double DEFAULT '10'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  RANDOM DISTRIBUTE */"));

		rootConnection.execute("CREATE TABLE `pe73567_t4` (`a` double DEFAULT 10.5 )");
		rootConnection.assertResults(
				"show create table pe73567_t4",
				br(nr, "pe73567_t4",
						"CREATE TABLE `pe73567_t4` (\n  `a` double DEFAULT '10.5'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  RANDOM DISTRIBUTE */"));

		rootConnection.execute("CREATE TABLE `pe73567_t5` (`a` varchar(14) DEFAULT 'Hello, World!' ) DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci");
		rootConnection
				.assertResults(
						"show create table pe73567_t5",
						br(nr,
								"pe73567_t5",
								"CREATE TABLE `pe73567_t5` (\n  `a` varchar(14) DEFAULT 'Hello, World!'\n) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci /*#dve  RANDOM DISTRIBUTE */"));
	}
	
	@Test
	public void testDVE1592() throws Throwable {
		rootConnection.execute("create table dve1592 (id int primary key, fid int unique, sid int key)");
//		System.out.println(rootConnection.printResults("show create table dve1592"));
//		System.out.println(rootConnection.printResults("show keys in dve1592"));
		rootConnection.assertResults("show keys in dve1592",
				br(nr,"dve1592",0,"fid",1,"fid",ignore,ignore,ignore,ignore,ignore,ignore,ignore,ignore,
				   nr,"dve1592",0,"PRIMARY",1,"id",ignore,ignore,ignore,ignore,ignore,ignore,ignore,ignore,
				   nr,"dve1592",1,"sid",1,"sid",ignore,ignore,ignore,ignore,ignore,ignore,ignore,ignore));
	}
	
	@Test
	public void testPE1632() throws Throwable {
		rootConnection.execute("DROP TABLE IF EXISTS pe1632;");
		rootConnection.execute("SET SQL_MODE='TRADITIONAL'");
		
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				rootConnection
						.execute("CREATE TABLE t1 (c1 VARCHAR(10) NOT NULL COMMENT 'c1 comment', c2 INTEGER,c3 INTEGER COMMENT '012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER, INDEX i1 (c1) COMMENT 'i1 comment',INDEX i2(c2)"
				+ ") COMMENT='abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcde'");
			}
		}.assertError(SchemaException.class, MySQLErrors.tooLongTableCommentFormatter, "t1", 2048L);
	}
}
