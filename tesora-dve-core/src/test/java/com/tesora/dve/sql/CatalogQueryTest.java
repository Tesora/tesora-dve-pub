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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.standalone.PETest;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CatalogQueryTest extends SchemaTest {

	private static final int SITES = 4;

	private static final StorageGroupDDL pg = new StorageGroupDDL("cqt",SITES,"cqtps"); 
	
	private static final PEDDL project = 
		new PEDDL("cqtdb",pg,"database");
		
	private static final PEDDL second =
		new PEDDL("second",pg,"database");
	
	private static final PEDDL mtproject =
		new PEDDL("mtcqtdb", new StorageGroupDDL("mtcqt",SITES,"mtcqtps"),"database").withMTMode(MultitenantMode.ADAPTIVE);
	
	private static final PEDDL[] projects = new PEDDL[] { project, mtproject };
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(project, second, mtproject);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	PortalDBHelperConnectionResource conn = null;
	
	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		for(int i = 0; i < projects.length; i++) {
			projects[i].clearCreated();
			projects[i].create(conn);
			if (projects[i] == project) {
				conn.execute(second.getCreateDatabaseStatement());
			}
		}
	}
	
	@After
	public void after() throws Throwable {
		if (conn != null && conn.isConnected()) {
			for (int i = 0; i < projects.length; i++) {
				if (projects[i] == project) {
					conn.execute("drop database if exists " + second.getDatabaseName());
				}
				projects[i].destroy(conn);
			}
			conn.disconnect();
		}
		
		conn = null;
	}
	
	@Test
	public void AtestCatalogQueries() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		conn.execute("create range offlimits (int,int) persistent group cqtps");
		conn.execute("create table `A` (`id` int, `desc` varchar(50)) random distribute");
		conn.execute("create table `AB` (`id1` int, `id2` int, `desc` varchar(50)) range distribute on (`id1`,`id2`) using offlimits");

		conn.execute("set session dve_metadata_extensions = 1");
				
		conn.disconnect();
		// reset the current database
		conn.connect();
		// databases
		Object[] dbs = br(nr,"cqtdb",nr,PEConstants.INFORMATION_SCHEMA_DBNAME, nr, PEConstants.LANDLORD_TENANT, nr, PEConstants.MYSQL_SCHEMA_DBNAME, nr, "second");
		testCommand(conn,"database",dbs,"cqtdb",0,br(nr,"cqtdb"),false);
		testCommand(conn,"schema",dbs,"cqtdb",0,br(nr,"cqtdb"),false);

        String defaultCharSet = Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet();
        String defaultCollation = Singletons.require(HostService.class).getDBNative().getDefaultServerCollation();

		Object[] dbgenres = br(nr,"cqtdb","cqtps", SchemaTest.getIgnore(), SchemaTest.getIgnore(),"off","strict",defaultCharSet,defaultCollation,
				nr,PEConstants.INFORMATION_SCHEMA_DBNAME,PEConstants.INFORMATION_SCHEMA_GROUP_NAME, SchemaTest.getIgnore(), SchemaTest.getIgnore(),"off","strict",defaultCharSet,defaultCollation,
				nr,PEConstants.LANDLORD_TENANT,"mtcqtps",SchemaTest.getIgnore(), SchemaTest.getIgnore(), "adaptive","strict",defaultCharSet,defaultCollation,
				nr,PEConstants.MYSQL_SCHEMA_DBNAME,PEConstants.INFORMATION_SCHEMA_GROUP_NAME,SchemaTest.getIgnore(),SchemaTest.getIgnore(),"off","strict",defaultCharSet,defaultCollation,
				nr,"second","cqtps",SchemaTest.getIgnore(),SchemaTest.getIgnore(), "off","strict",defaultCharSet,defaultCollation);
		Object[] dbextgenres = br(nr,"cqtdb","cqtps",SchemaTest.getIgnore(), SchemaTest.getIgnore(),"off","strict",defaultCharSet,defaultCollation);
		testCommand(conn,"database",dbgenres,"cqtdb",0,dbextgenres,true);
		testCommand(conn,"schema",dbgenres,"cqtdb",0,dbextgenres,true);
		// the integral value is the id of the generation, since generations don't have names
		testCommand(conn,"persistent group",
				br(nr,"cqtps",new Integer(3), 
				   nr, PEConstants.INFORMATION_SCHEMA_GROUP_NAME, new Integer(1),
				   nr, "mtcqtps", new Integer(4),
				   nr,PEConstants.SYSTEM_GROUP_NAME,new Integer(2)),
				   "cqtps",0,
				   br(nr,"cqtps",new Integer(3)),false);
		testCommand(conn,"persistent group",
				br(nr,"cqtps",new Integer(3), 
				   nr, PEConstants.INFORMATION_SCHEMA_GROUP_NAME, new Integer(1),
				   nr, "mtcqtps", new Integer(4),
				   nr,PEConstants.SYSTEM_GROUP_NAME,new Integer(2)),
				   "cqtps",0,br(nr,"cqtps",new Integer(3)),true);
		String siteURL = TestCatalogHelper.getInstance().getCatalogUrl();
		Object[] siteResults = br(nr,"cqt0","Single",siteURL, 
				nr, "cqt1", "Single", siteURL, 
				nr, "cqt2", "Single", siteURL, 
				nr, "cqt3", "Single", siteURL, 
				nr, "mtcqt0", "Single", siteURL, 
				nr, "mtcqt1", "Single", siteURL, 
				nr, "mtcqt2", "Single", siteURL, 
				nr, "mtcqt3", "Single", siteURL, 
				nr, PEConstants.SYSTEM_SITENAME, "Single", siteURL);
		testCommand(conn,"persistent site",siteResults,"cqt1", 1, br(nr, "cqt1", "Single", siteURL),false);
		testCommand(conn,"persistent site",siteResults,"cqt1", 1, br(nr, "cqt1", "Single", siteURL),true);
	
		Integer zero = new Integer(0);
		ResourceResponse rr = conn.fetch("show generation sites where Persistent_Group='cqtps'");
		List<ResultRow> results = rr.getResults();
		HashSet<String> sitenames = new HashSet<String>(Arrays.asList(new String[] { "cqt0", "cqt1", "cqt2", "cqt3" }));
		
		assertEquals("number of rows",4,results.size());
		for(ResultRow row : results) {
			ResultColumn sgname = row.getResultColumn(1);
			ResultColumn genversion = row.getResultColumn(2);
			ResultColumn sitename = row.getResultColumn(3);
			assertEquals("cqtps",sgname.getColumnValue());
			assertEquals(zero,genversion.getColumnValue());
			String sn = (String) sitename.getColumnValue();
			assertTrue("site should match",sitenames.contains(sn));
		}		

		testCommand(conn,"generation",
				br(nr,new Integer(1),PEConstants.INFORMATION_SCHEMA_GROUP_NAME,zero,"NO",
				   nr,new Integer(2),PEConstants.SYSTEM_GROUP_NAME,zero,"NO",
				   nr,new Integer(3),"cqtps",zero,"NO",
				   nr,new Integer(4),"mtcqtps",zero,"NO"),
				"cqtps",-1,null,true);
						
						
		testCommand(conn,"range",br(nr,"offlimits","cqtps","int,int"),"offlimits",0,br(nr,"offlimits","cqtps","int,int"),false);
		testCommand(conn,"range",br(nr,"offlimits","cqtps","int,int"),"offlimits",0,br(nr,"offlimits","cqtps","int,int"),true);
		
		try {
			testCommand(conn,"table",br(nr,"A","AB"),"A",0,br(nr,"A"),false);
			fail("Should have no results on show tables with no database set");
		} catch (SQLException e) {
			assertSQLException(e,MySQLErrors.missingDatabaseFormatter,"No database selected");
		}

		conn.execute("use cqtdb");
		testCommand(conn,"table",br(nr,"A",nr,"AB"),"A",0,br(nr,"A"),false);
		testCommand(conn,"table",br(nr, "A", "Random", "cqtps", 
				                    nr, "AB", "Range", "cqtps"),
				"AB",1,br(nr,"AB","Range","cqtps"),true);
		

		conn.execute("set session dve_metadata_extensions = 1");
		
		Object[] abCols = br(
				nr, "id1", "int(11)", "YES", "", null, "",
				nr, "id2", "int(11)", "YES", "", null, "",
				nr,"desc", "varchar(50)", "YES", "", null, "");
		conn.assertResults("show columns in AB",abCols);
		conn.assertResults("describe AB",abCols);

		conn.assertResults("show full columns in AB",
				br(	nr, "id1", "int(11)", null, "YES", "", null, "", "", "",
					nr, "id2", "int(11)", null, "YES", "", null, "", "", "",
					nr, "desc", "varchar(50)", "utf8_general_ci", "YES", "", null, "", "", ""));

		conn.assertResults("show dynamic site providers", 
				br( nr,OnPremiseSiteProvider.DEFAULT_NAME, OnPremiseSiteProvider.class.getCanonicalName(), "YES"));
	
		// TODO
		// unclear how this would work - maybe a postexecution filter?
		if (false) {
			try {
				conn.execute("describe foo");
				fail("describe foo for missing foo should throw");
			} catch (SQLException e) {
				assertSQLException(e,MySQLErrors.missingTableFormatter,
						"Table 'cqtdb.foo' doesn't exist");
			}
		}
	}
	
	// plural results are what you get from the plural forms, which is traditionally just the
	// name for some commands, and 
	private void testCommand(PortalDBHelperConnectionResource conn1,
			String singular, Object[] pluralResults, String named, int offset, Object[] singularResults, boolean ext) throws Throwable {
		conn1.execute("set session dve_metadata_extensions = " + (ext ? "true" : "false"));
		String q1 = "show " + singular + "s";
		String q2 = "show " + singular + " " + named;
		String q3 = "show " + singular + "s like '" + named + "'";
//		System.out.println(conn.printResults(q1));
		conn1.assertResults(q1,pluralResults);
//		System.out.println(conn.printResults(q2));
		if (singularResults != null)
			conn1.assertResults(q2,singularResults);
		if (offset > -1) {
			Object[] namedResults = (Object[])pluralResults[offset];
			//		System.out.println(conn.printResults(q3));
			conn1.assertResults(q3,new Object[] { namedResults });
		}
	}

	@Test
	public void testInfoSchema() throws Throwable {
		exerciseSchema(conn,"information_schema","character_sets");
	}
	
	@Test
	public void testMysqlSchema() throws Throwable {
		exerciseSchema(conn,"mysql");
	}

	
	public static void exerciseSchema(ConnectionResource conn, String whichOne, String ...notTables) throws Throwable {
		conn.execute("use " + whichOne);
		SchemaTest.echo(conn.printResults("show tables"));
		ResourceResponse rr = conn.fetch("show tables");
		List<ResultRow> results = rr.getResults();
		List<String> tabnames = Functional.apply(results, new UnaryFunction<String, ResultRow>() {

			@Override
			public String evaluate(ResultRow object) {
				return (String)object.getResultColumn(1).getColumnValue();
			}
			
		});
		HashSet<String> not = (notTables == null ? new HashSet<String>() : new HashSet<String>(Arrays.asList(notTables)));
		
		for(String tn : tabnames) {
			if (not.contains(tn) || not.contains(tn.toLowerCase())) continue;
			if (SchemaTest.isNoisy()) {
				SchemaTest.echo(conn.printResults("describe " + tn));
				SchemaTest.echo(conn.printResults("select * from " + tn));
			} else {
				conn.execute("describe " + tn);
				conn.execute("select * from " + tn);
			}
		}		
	}
	
	@Test
	public void exerciseMysqlSchema() throws Throwable {
		conn.execute("use " + mtproject.getDatabaseName());
		conn.execute("create tenant t1 'first terminator' on " + mtproject.getDatabaseName());
		conn.execute("create tenant t2 'second terminator'");
		String hostName = "localhost";
		String[] users = new String[] { "cqtu1", "cqtu2" };
		for(String u : users) {
			String udesc = "'" + u + "'@'" + hostName + "' identified by '" + u + "'";
			removeUser(conn,u,hostName);
			conn.execute("create user " + udesc);
			conn.execute("grant all on t1.* to " + udesc);
			if (!"cqtu2".equals(u))
				conn.execute("grant all on t2.* to " + udesc);
		}
		conn.execute("use mysql");
		conn.assertResults("select * from db order by Db, User", 
				br(nr,"localhost","t1","cqtu1",
						nr,"localhost","t1","cqtu2",
						nr,"localhost","t2","cqtu1"));
		conn.assertResults("select User from db where Db = 't2' order by User",	br(nr,"cqtu1"));
		conn.assertResults("select count(*) from db where Db = 't1'",br(nr,2L));
	}
	
	@Test
	public void exerciseShowTableStatus() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		// before there are any table this should return an empty result set
		conn.assertResults("show table status", br());
		
		conn.execute("create table one (`id` int auto_increment, `foo` varchar(32), primary key (`id`)) random distribute");
		conn.execute("insert into one (foo) values ('a'),('b'),('c'),('d'),('e')");
		ResourceResponse rr = conn.fetch("show table status like 'one'");
		
		List<ResultRow> rows = rr.getResults();
		assertEquals(1,rows.size());
		ResultRow fr = rows.get(0);
		assertEquals("one",fr.getResultColumn(1).getColumnValue());
		assertEquals("InnoDB",fr.getResultColumn(2).getColumnValue());
		
		// Test PE-284 no like specified should return all tables
		rr = conn.fetch("show table status");
		rows = rr.getResults();
		assertEquals(1,rows.size());
		fr = rows.get(0);
		assertEquals("one",fr.getResultColumn(1).getColumnValue());
		assertEquals("InnoDB",fr.getResultColumn(2).getColumnValue());
		
		conn.disconnect();
		// reset the current database
		conn.connect();
		
		try {
			conn.execute("show table status");
		} catch (SQLException e) {
			assertSQLException(e,MySQLErrors.missingDatabaseFormatter,
					"No database selected");
		}
				
		rr = conn.fetch("show table status from " + project.getDatabaseName());
		rows = rr.getResults();
		assertEquals(1,rows.size());
		fr = rows.get(0);
		assertEquals("one",fr.getResultColumn(1).getColumnValue());
		assertEquals("InnoDB",fr.getResultColumn(2).getColumnValue());

		conn.assertResults("show table status from junk", br());

		try {
			conn.execute("show tables");
		} catch (SQLException e) {
			assertSQLException(e,MySQLErrors.missingDatabaseFormatter,
					"No database selected");
		}
		
		rr = conn.fetch("show tables from " + project.getDatabaseName());
		rows = rr.getResults();
		assertEquals(1,rows.size());
		fr = rows.get(0);
		assertEquals("one",fr.getResultColumn(1).getColumnValue());
	}

	@Test
	public void testPE201And313() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		String decl = "create table dt (`id` int, `sid` int, `stuff` varchar(32) not null default 'toldyaso', primary key (`id`))";
		conn.execute(decl);
		conn.execute("use second");
		conn.execute(decl);
		conn.assertResults("describe dt", br(nr,"id","int(11)","NO","PRI",getIgnore(), "",
				nr,"sid","int(11)","YES","",getIgnore(),"",
				nr,"stuff","varchar(32)","NO","","toldyaso",""));
		conn.assertResults("show full columns in dt",
				br(nr,"id","int(11)",getIgnore(),"NO","PRI",getIgnore(), "",getIgnore(),getIgnore(),
						nr,"sid","int(11)",getIgnore(),"YES","",getIgnore(),"",getIgnore(),getIgnore(),
						nr,"stuff","varchar(32)",getIgnore(),"NO","","toldyaso","",getIgnore(),getIgnore()));
		String scts = AlterTest.getCreateTable(conn, "dt");
		conn.execute("use " + project.getDatabaseName());
		String fcts = AlterTest.getCreateTable(conn, "dt");
		assertEquals("same def, different dbs, should still have same create table stmt",scts,fcts);
		scts = AlterTest.getCreateTable(conn, "second.dt");
		assertEquals("same def, different dbs, qualified show should still have same create table stmt",fcts,scts);
	}
	
	@Test 
	public void testDescribeColumns() throws Throwable {
		// using this as the guide to all the combination of datatype precision/scale
		//data_type:
		//    BIT[(length)]
		//  | TINYINT[(length)] [UNSIGNED] [ZEROFILL]
		//  | SMALLINT[(length)] [UNSIGNED] [ZEROFILL]
		//  | MEDIUMINT[(length)] [UNSIGNED] [ZEROFILL]
		//  | INT[(length)] [UNSIGNED] [ZEROFILL]
		//  | INTEGER[(length)] [UNSIGNED] [ZEROFILL]
		//  | BIGINT[(length)] [UNSIGNED] [ZEROFILL]
		//  | REAL[(length,decimals)] [UNSIGNED] [ZEROFILL]
		//  | DOUBLE[(length,decimals)] [UNSIGNED] [ZEROFILL]
		//  | FLOAT[(length,decimals)] [UNSIGNED] [ZEROFILL]
		//  | DECIMAL[(length[,decimals])] [UNSIGNED] [ZEROFILL]
		//  | NUMERIC[(length[,decimals])] [UNSIGNED] [ZEROFILL]
		//  | DATE
		//  | TIME
		//  | TIMESTAMP
		//  | DATETIME
		//  | YEAR
		//  | CHAR[(length)]
		//      [CHARACTER SET charset_name] [COLLATE collation_name]
		//  | VARCHAR(length)
		//      [CHARACTER SET charset_name] [COLLATE collation_name]
		//  | BINARY[(length)]
		//  | VARBINARY(length)
		//  | TINYBLOB
		//  | BLOB
		//  | MEDIUMBLOB
		//  | LONGBLOB
		//  | TINYTEXT [BINARY]
		//      [CHARACTER SET charset_name] [COLLATE collation_name]
		//  | TEXT [BINARY]
		//      [CHARACTER SET charset_name] [COLLATE collation_name]
		//  | MEDIUMTEXT [BINARY]
		//      [CHARACTER SET charset_name] [COLLATE collation_name]
		//  | LONGTEXT [BINARY]
		//      [CHARACTER SET charset_name] [COLLATE collation_name]
		StringBuffer table = new StringBuffer();
		table.append("CREATE TABLE pe202alltypes(")
				.append("a BIT")
				.append(",b BIT(5)")
				.append(",c TINYINT")
				.append(",d TINYINT(5)")
				.append(",e SMALLINT")
				.append(",f SMALLINT(5)")
				.append(",g MEDIUMINT")
				.append(",h MEDIUMINT(5)")
				.append(",i INT")
				.append(",j INT(5)")
				.append(",k INTEGER")
				.append(",l INTEGER(5)")
				.append(",m BIGINT")
				.append(",n BIGINT(5)")
				.append(",o REAL")
				.append(",p REAL(6,5)")
				.append(",q DOUBLE")
				.append(",r DOUBLE(6,5)")
				.append(",s FLOAT")
				.append(",t FLOAT(6,5)")
				.append(",u DECIMAL")
				.append(",ud DECIMAL(6)")
				.append(",v DECIMAL(6,5)")
				.append(",w NUMERIC")
				.append(",x NUMERIC(6)")
				.append(",y NUMERIC(6,5)")
				.append(",z DATE")
				.append(",aa TIME")
				.append(",bb TIMESTAMP")
				.append(",cc DATETIME")
				.append(",dd YEAR")
				.append(",ee CHAR")
				.append(",ff CHAR(5)")
				.append(",gg VARCHAR(5)")
				.append(",hh BINARY")
				.append(",ii BINARY(5)")
				.append(",jj VARBINARY(5)")
				.append(",kk TINYBLOB")
				.append(",ll BLOB")
				.append(",mm MEDIUMBLOB")
				.append(",nn LONGBLOB")
				.append(",oo TINYTEXT")
				.append(",pp TEXT")
				.append(",qq MEDIUMTEXT")
				.append(",rr LONGTEXT")
				.append(")");
		conn.execute(table.toString());
						      
		conn.assertResults("describe pe202alltypes",
				br(	nr, "a", "bit(1)", "YES", "", null, "",
					nr, "b", "bit(5)", "YES", "", null, "",
					nr, "c", "tinyint(4)", "YES", "", null, "",
					nr, "d", "tinyint(5)", "YES", "", null, "",
					nr, "e", "smallint(6)", "YES", "", null, "",
					nr, "f", "smallint(5)", "YES", "", null, "",
					nr, "g", "mediumint(9)", "YES", "", null, "",
					nr, "h", "mediumint(5)", "YES", "", null, "",
					nr, "i", "int(11)", "YES", "", null, "",
					nr, "j", "int(5)", "YES", "", null, "",
					nr, "k", "int(11)", "YES", "", null, "",
					nr, "l", "int(5)", "YES", "", null, "",
					nr, "m", "bigint(20)", "YES", "", null, "",
					nr, "n", "bigint(5)", "YES", "", null, "",
					nr, "o", "double precision", "YES", "", null, "",
					nr, "p", "double precision(6,5)", "YES", "", null, "",
					nr, "q", "double", "YES", "", null, "",
					nr, "r", "double(6,5)", "YES", "", null, "",
					nr, "s", "float", "YES", "", null, "",
					nr, "t", "float(6,5)", "YES", "", null, "",
					nr, "u", "decimal(10,0)", "YES", "", null, "",
					nr, "ud", "decimal(6,0)", "YES", "", null, "",
					nr, "v", "decimal(6,5)", "YES", "", null, "",
					nr, "w", "decimal(10,0)", "YES", "", null, "",
					nr, "x", "decimal(6,0)", "YES", "", null, "",
					nr, "y", "decimal(6,5)", "YES", "", null, "",
					nr, "z", "date", "YES", "", null, "",
					nr, "aa", "time", "YES", "", null, "",
					nr, "bb", "timestamp", "NO", "", "CURRENT_TIMESTAMP", "on update CURRENT_TIMESTAMP",
					nr, "cc", "datetime", "YES", "", null, "",
					nr, "dd", "year(4)", "YES", "", null, "",
					nr, "ee", "char(1)", "YES", "", null, "",
					nr, "ff", "char(5)", "YES", "", null, "",
					nr, "gg", "varchar(5)", "YES", "", null, "",
					nr, "hh", "binary(1)", "YES", "", null, "",
					nr, "ii", "binary(5)", "YES", "", null, "",
					nr, "jj", "varbinary(5)", "YES", "", null, "",
					nr, "kk", "tinyblob", "YES", "", null, "",
					nr, "ll", "blob", "YES", "", null, "",
					nr, "mm", "mediumblob", "YES", "", null, "",
					nr, "nn", "longblob", "YES", "", null, "",
					nr, "oo", "tinytext", "YES", "", null, "",
					nr, "pp", "text", "YES", "", null, "",
					nr, "qq", "mediumtext", "YES", "", null, "",
					nr, "rr", "longtext", "YES", "", null, ""
					));
	}
	
	@Test
	public void testPHPAdminConnect() throws Throwable {
		conn.execute("create user 'nonroot'@'localhost' identified by 'password'");
		conn.execute("GRANT ALL ON " + project.getDatabaseName() + ".* TO 'nonroot'@'localhost'");
		ProxyConnectionResource nonRootConn = new ProxyConnectionResource("nonroot", "password");
		try {
			// this just tests to make sure we don't blow up executing these statements...
			nonRootConn.execute("use " + project.getDatabaseName());
			nonRootConn.execute("create table foo (`col1` int, `col2` int, `col3` varchar(32) not null default 'toldyaso', primary key (`col1`))");
			nonRootConn.execute("insert into foo values (1, 1, 'one')");
			nonRootConn.execute("create table bar (`col1` int, `col2` int, `col3` varchar(32) not null default 'toldyaso', primary key (`col1`))");
			nonRootConn.execute("insert into bar values (1, 2, 'two'),(3, 3, 'three')");
			
			ResourceResponse rr = nonRootConn.fetch("SET CHARACTER SET 'utf8'");
			assertEquals(0,rr.getResults().size());
			
			rr = nonRootConn.fetch("SET collation_connection = 'utf8_general_ci'");
			assertEquals(0,rr.getResults().size());
			
			rr = nonRootConn.fetch("SHOW PLUGINS");
			assertTrue(rr.getResults().size() > 0);
			
			nonRootConn.assertResults("select * from foo,bar where foo.col1 = bar.col1 LIMIT 0, 30",
					br(nr,1,1,"one",1,2,"two"));
			
			nonRootConn.execute("select SQL_CALC_FOUND_ROWS * from foo, bar where foo.col1 = bar.col1 LIMIT 1");
			
			nonRootConn.execute("SELECT FOUND_ROWS()");
			
			rr = nonRootConn.fetch("SHOW TABLE STATUS FROM `" + project.getDatabaseName() +"` LIKE 'foo%'");
			assertEquals(1,rr.getResults().size());

			// TODO:
			// we are in mt mode, so we can't use the naked database name on the nonroot conn
			/*
			
			nonRootConn.assertResults("SHOW FULL COLUMNS FROM `" + project.getDatabaseName() +"`.`foo`",
					br(nr, "col1", "int(11)", "", "NO", "PRI", null, "", "", "",
					   nr, "col2", "int(11)", "", "YES", "", null, "", "", "",
					   nr, "col3", "varchar(32)", "", "NO", "", "toldyaso", "", "", ""));

			nonRootConn.assertResults("SHOW FULL COLUMNS FROM `" + project.getDatabaseName() +"`.`bar`",
					br(nr, "col1", "int(11)", "", "NO", "PRI", null, "", "", "",
					   nr, "col2", "int(11)", "", "YES", "", null, "", "", "",
					   nr, "col3", "varchar(32)", "", "NO", "", "toldyaso", "", "", ""));
			*/
			
			nonRootConn.assertResults("SELECT USER()", br(nr, "nonroot@localhost"));

			try {
				nonRootConn.execute("show master logs");
			} catch (SchemaException e) {
				assertErrorInfo(e,MySQLErrors.internalFormatter,
						"Internal error: You do not have permission to show persistent sites");
			}

			try {
				nonRootConn.execute("show master status");
			} catch (SchemaException e) {
				assertErrorInfo(e,MySQLErrors.internalFormatter,
						"Internal error: You do not have permission to show persistent sites");
			}

			try {
				nonRootConn.execute("show slave status");
			} catch (SchemaException e) {
				assertErrorInfo(e,MySQLErrors.internalFormatter,
						"Internal error: You do not have permission to show persistent sites");
			}
			
			nonRootConn.assertResults("SELECT * FROM information_schema.character_sets", 
					br(nr, "ascii", "US ASCII", Long.valueOf(1),
					   nr, "latin1", "cp1252 West European", Long.valueOf(1),
					   nr, "utf8", "UTF-8 Unicode", Long.valueOf(3),
					   nr, "utf8mb4", "UTF-8 Unicode", Long.valueOf(4)));
			
			nonRootConn.assertResults("SELECT * FROM information_schema.collations", br(
					nr, "ascii_general_ci", "ascii", Long.valueOf(11), "Yes", "Yes", Long.valueOf(1),
					nr, "ascii_bin", "ascii", Long.valueOf(65), "", "Yes", Long.valueOf(1),
					nr, "latin1_swedish_ci", "latin1", Long.valueOf(8), "Yes", "Yes", Long.valueOf(1),
					nr, "latin1_danish_ci", "latin1", Long.valueOf(15), "", "Yes", Long.valueOf(1),
					nr, "latin1_german2_ci", "latin1", Long.valueOf(31), "", "Yes", Long.valueOf(2),
					nr, "latin1_bin", "latin1", Long.valueOf(47), "", "Yes", Long.valueOf(1),
					nr, "latin1_general_ci", "latin1", Long.valueOf(48), "", "Yes", Long.valueOf(1),
					nr, "latin1_general_cs", "latin1", Long.valueOf(49), "", "Yes", Long.valueOf(1),
					nr, "latin1_spanish_ci", "latin1", Long.valueOf(94), "", "Yes", Long.valueOf(1),
					nr, "utf8_general_ci", "utf8", Long.valueOf(33), "Yes", "Yes", Long.valueOf(1),
					nr, "utf8_bin", "utf8", Long.valueOf(83), "", "Yes", Long.valueOf(1),
					nr, "utf8_unicode_ci", "utf8", Long.valueOf(192), "", "Yes", Long.valueOf(8),
					nr, "utf8_icelandic_ci", "utf8", Long.valueOf(193), "", "Yes", Long.valueOf(8),
					nr, "utf8_latvian_ci", "utf8", Long.valueOf(194), "", "Yes", Long.valueOf(8),
					nr, "utf8_romanian_ci", "utf8", Long.valueOf(195), "", "Yes", Long.valueOf(8),
					nr, "utf8_slovenian_ci", "utf8", Long.valueOf(196), "", "Yes", Long.valueOf(8),
					nr, "utf8_polish_ci", "utf8", Long.valueOf(197), "", "Yes", Long.valueOf(8),
					nr, "utf8_estonian_ci", "utf8", Long.valueOf(198), "", "Yes", Long.valueOf(8),
					nr, "utf8_spanish_ci", "utf8", Long.valueOf(199), "", "Yes", Long.valueOf(8),
					nr, "utf8_swedish_ci", "utf8", Long.valueOf(200), "", "Yes", Long.valueOf(8),
					nr, "utf8_turkish_ci", "utf8", Long.valueOf(201), "", "Yes", Long.valueOf(8),
					nr, "utf8_czech_ci", "utf8", Long.valueOf(202), "", "Yes", Long.valueOf(8),
					nr, "utf8_danish_ci", "utf8", Long.valueOf(203), "", "Yes", Long.valueOf(8),
					nr, "utf8_lithuanian_ci", "utf8", Long.valueOf(204), "", "Yes", Long.valueOf(8),
					nr, "utf8_slovak_ci", "utf8", Long.valueOf(205), "", "Yes", Long.valueOf(8),
					nr, "utf8_spanish2_ci", "utf8", Long.valueOf(206), "", "Yes", Long.valueOf(8),
					nr, "utf8_roman_ci", "utf8", Long.valueOf(207), "", "Yes", Long.valueOf(8),
					nr, "utf8_persian_ci", "utf8", Long.valueOf(208), "", "Yes", Long.valueOf(8),
					nr, "utf8_esperanto_ci", "utf8", Long.valueOf(209), "", "Yes", Long.valueOf(8),
					nr, "utf8_hungarian_ci", "utf8", Long.valueOf(210), "", "Yes", Long.valueOf(8),
					nr, "utf8_sinhala_ci", "utf8", Long.valueOf(211), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_general_ci", "utf8mb4", Long.valueOf(45), "Yes", "Yes", Long.valueOf(1),
					nr, "utf8mb4_bin", "utf8mb4", Long.valueOf(46), "", "Yes", Long.valueOf(1),
					nr, "utf8mb4_unicode_ci", "utf8mb4", Long.valueOf(224), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_icelandic_ci", "utf8mb4", Long.valueOf(225), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_latvian_ci", "utf8mb4", Long.valueOf(226), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_romanian_ci", "utf8mb4", Long.valueOf(227), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_slovenian_ci", "utf8mb4", Long.valueOf(228), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_polish_ci", "utf8mb4", Long.valueOf(229), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_estonian_ci", "utf8mb4", Long.valueOf(230), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_spanish_ci", "utf8mb4", Long.valueOf(231), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_swedish_ci", "utf8mb4", Long.valueOf(232), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_turkish_ci", "utf8mb4", Long.valueOf(233), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_czech_ci", "utf8mb4", Long.valueOf(234), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_danish_ci", "utf8mb4", Long.valueOf(235), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_lithuanian_ci", "utf8mb4", Long.valueOf(236), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_slovak_ci", "utf8mb4", Long.valueOf(237), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_spanish2_ci", "utf8mb4", Long.valueOf(238), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_roman_ci", "utf8mb4", Long.valueOf(239), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_persian_ci", "utf8mb4", Long.valueOf(240), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_esperanto_ci", "utf8mb4", Long.valueOf(241), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_hungarian_ci", "utf8mb4", Long.valueOf(242), "", "Yes", Long.valueOf(8),
					nr, "utf8mb4_sinhala_ci", "utf8mb4", Long.valueOf(243), "", "Yes", Long.valueOf(8)
					));

			nonRootConn.assertResults("SELECT * FROM information_schema.events", br());
			
			nonRootConn.assertResults("SELECT * FROM information_schema.routines", br());

            nonRootConn.assertResults(
					"SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '" + project.getDatabaseName() + "' LIMIT 1;",
					br(nr, Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet()));

		} finally {
			nonRootConn.disconnect();
			nonRootConn.close();
		}
	}
		
	@Test
	public void testScopedNameOnShowCreateTable() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		conn.execute("create table sct (`id` int, `junk` varchar(32), primary key (`id`))");
		String expected = conn.printResults("show create table sct");
		String qsql = "show create table `" + project.getDatabaseName() + "`.`sct`";
		String actual = conn.printResults(qsql);
		assertEquals("should have same result set regardless of qualification",expected,actual);
	}

	@Test
	public void testPE616() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		conn.execute("create range openrange(int) persistent group " + project.getPersistentGroup().getName());
		String body = " (`id` int auto_increment, `row` varchar(32), primary key(`id`)) ";
		String[] tabnames = { "taba", "tabb", "tabs", "tabr" };
		String[] vects = { "random distribute","broadcast distribute","static distribute on (`id`)","range distribute on (`id`) using openrange" };
		for(int i = 0; i < tabnames.length; i++) {
			conn.execute("create table `" + tabnames[i] + "`" + body + vects[i]);
		}
		String values = " (`row`) values ('first'),('second'),('third')";
		for(int i = 0; i < tabnames.length; i++)
			conn.execute("insert into `" + tabnames[i] + "`" + values);
		String[] bodies = new String[tabnames.length];
		for(int i = 0; i < tabnames.length; i++) {
			bodies[i] = AlterTest.getCreateTable(conn, tabnames[i]);
			assertTrue("autoinc is specified",bodies[i].indexOf("AUTO_INCREMENT") > -1);
//			System.out.println(bodies[i]);
		}
		for(int i = 0; i < tabnames.length; i++) {
			conn.execute("drop table `" + tabnames[i] + "`");
		}
		// and now recreate using the stored create tables
		for(int i = 0; i < bodies.length; i++) {
			conn.execute(bodies[i]);
		}
		for(int i = 0; i < tabnames.length; i++) {
			assertEquals("has same table decl",bodies[i],AlterTest.getCreateTable(conn, tabnames[i]));
		}
	}

	@Test
	public void testDistributionsTable() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		conn.execute("create range openrange(int) persistent group " + project.getPersistentGroup().getName());
		String body = " (`id` int auto_increment, `row` varchar(32), primary key(`id`)) ";
		String[] tabnames = { "taba", "tabb", "tabs", "tabr" };
		String[] vects = { "random distribute","broadcast distribute","static distribute on (`id`)","range distribute on (`id`) using openrange" };
		for(int i = 0; i < tabnames.length; i++) {
			conn.execute("create table `" + tabnames[i] + "`" + body + vects[i]);
		}
		conn.assertResults("select * from information_schema.distributions where database_name = 'cqtdb' order by database_name, table_name, vector_position", 
				br(nr,"cqtdb","taba",null,null,"Random",null,
				   nr,"cqtdb","tabb",null,null,"Broadcast",null,
				   nr,"cqtdb","tabr","id",new Integer(1),"Range","openrange",
				   nr,"cqtdb","tabs","id",new Integer(1),"Static",null));
	}

	@Test
	public void literalExpressionInfoSchemaTest() throws Throwable {
		conn.assertResults("select 'text' from information_schema.schemata limit 1", br(nr,"text"));
		int intVal = Integer.MAX_VALUE;
		conn.assertResults("select " + intVal + " from information_schema.schemata limit 1", br(nr,Long.valueOf(intVal)));
		intVal = Integer.MIN_VALUE;
		conn.assertResults("select " + intVal + " from information_schema.schemata limit 1", br(nr,Long.valueOf(intVal)));
		double doubleVal = 1111111111.11111;
		conn.assertResults("select " + doubleVal + " from information_schema.schemata limit 1", br(nr,doubleVal));
		doubleVal = -0.0000000000000000000001;
		conn.assertResults("select " + doubleVal + " from information_schema.schemata limit 1", br(nr,doubleVal));
		long longVal = Long.MAX_VALUE;
		conn.assertResults("select " + longVal + " from information_schema.schemata limit 1", br(nr,Long.valueOf(longVal)));
		longVal = Long.MIN_VALUE;
		conn.assertResults("select " + longVal + " from information_schema.schemata limit 1", br(nr,Long.valueOf(longVal)));
	}
	
	@Test
	public void testShowTemplates() throws Throwable {
		TemplateBuilder t1 = new TemplateBuilder("t1")
		.withTable(".*", "Broadcast");
		conn.assertResults("show templates", br());
		conn.execute(t1.toCreateStatement());
		conn.assertResults("show templates", br(nr,"t1",null,null,t1.toXml()));
		conn.execute("drop template t1");
		conn.assertResults("show templates", br());
		TemplateBuilder t2 = new TemplateBuilder("t2")
			.withRequirement("create range if not exists dodve_range (int) persistent group #sg#")
			.withRangeTable(".*fly", "dodve_range","dodvey");
		conn.execute(t2.toCreateStatement());
		conn.assertResults("show templates like '%t2%'", br(nr,"t2",null,null,t2.toXml()));
		t2 = new TemplateBuilder("t2")
			.withTable(".*fly", "Random");
		conn.execute(t2.toAlterStatement() + "match='whatever' comment 'explanation'");
		conn.assertResults("show templates",br(nr,"t2","whatever","explanation",t2.toXml()));
	}
	
	@Test
	public void DVE1497() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		conn.execute("create table dve1497targ (id int, fid int, sid int, primary key (id)) broadcast distribute");
		conn.execute("create table dve1497ref (id int, fid int, sid int, primary key (id), foreign key (fid) references dve1497targ (id)) broadcast distribute");
		conn.execute("set autocommit=0");
		conn.execute("start transaction");
		conn.assertResults("select id, fid, sid from dve1497ref order by id",br());
		conn.assertResults("select id, fid, sid from dve1497targ order by id",br());
		try (ProxyConnectionResource pcr = new ProxyConnectionResource()) {
			pcr.execute("use " + project.getDatabaseName());
			pcr.assertResults("show create table dve1497ref",br(nr,ignore,ignore));
		}
		conn.execute("commit");
		conn.execute("set autocommit=1");

	}
	
	@Test
	public void testPE1502Partial() throws Throwable {
		conn.execute("use " + project.getDatabaseName());
		conn.execute("create table pe1502(id int auto_increment, fid varchar(32), sid decimal(10,5), primary key (id), key (fid)) broadcast distribute");
		
		String sql = 
				String.format("select table_catalog, table_schema, column_name, column_type, column_key, privileges from information_schema.columns where table_schema = '%s' and table_name = '%s'",
						project.getDatabaseName(),"pe1502");
		
		conn.assertResults(sql,
				br(nr,"def","cqtdb","id","int(11)","PRI","",
				   nr,"def","cqtdb","fid","varchar(32)","MUL","",
				   nr,"def","cqtdb","sid","decimal(10,5)","",""));		
	}
	
}
