// OS_STATUS: public
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

import java.math.BigDecimal;

import com.tesora.dve.sql.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.standalone.PETest;

public class SelectTest extends SchemaTest {
	private static final ProjectDDL checkDDL = new PEDDL("checkdb", new StorageGroupDDL("check", 2, "checkg"), "schema");
	private static final ProjectDDL otherDDL = new PEDDL("otherdb", new StorageGroupDDL("other", 2, "otherg"), "database");
    private static final ProjectDDL singleDDL = new PEDDL("singledb", new StorageGroupDDL("single", 1, "singleg"), "schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL, otherDDL,singleDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected ProxyConnectionResource conn;
	protected DBHelperConnectionResource dbh;

	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
		dbh = new DBHelperConnectionResource();
	}

	@After
	public void disconnect() throws Throwable {
		if(conn != null) 
			conn.disconnect();
		conn = null;
		if(dbh != null)
			dbh.disconnect();
		dbh = null;
	}

        @Test
        public void test16BitLengthCodedString() throws Throwable {
            //this session string is 330 bytes long, so it takes multiple bytes to encode the length.  This test catches an issue where the length of 330 was decoded big-endian [as 18945] which generated an array out of bounds. -sgossard
            String sidText = "dhgi2rnupmlhggfsovvhubr4m1";
            String sessionText = "user_login|i:1;log_messages|a:1:{s:5:\"error\";a:1:{i:0;s:33:\"Invitatin Code field cannot empty\";}}top_product|s:2:\"10\";stat_from|s:8:\"01/01/13\";stat_until|s:8:\"01/08/13\";vertical|a:1:{i:2418;s:4:\"2418\";}InviteCodeVal|s:0:\"\";ValidateOpt|s:4:\"date\";date_member_invite_from|s:10:\"01/01/2013\";date_member_invite_until|s:10:\"01/08/2013\"";

            ProxyConnectionResource proxyConn = new ProxyConnectionResource();
            singleDDL.create(proxyConn);

            proxyConn.execute("CREATE TABLE `sessions` (  `uid` int(10) unsigned NOT NULL,  `sid` varchar(64) NOT NULL DEFAULT '',  `hostname` varchar(128) NOT NULL DEFAULT '',  `timestamp` int(11) NOT NULL DEFAULT '0',  `cache` int(11) NOT NULL DEFAULT '0',  `session` longtext,  PRIMARY KEY (`sid`),  KEY `timestamp` (`timestamp`),  KEY `uid` (`uid`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve  RANDOM DISTRIBUTE */");
            proxyConn.execute("INSERT INTO  `sessions` (sid, session) VALUES ('" + sidText+ "', '" + sessionText + "')");
            proxyConn.assertResults("SELECT s.sid,s.session FROM sessions s WHERE s.sid = '" + sidText+ "'",
                    br(nr, sidText,sessionText)
            );
            proxyConn.disconnect();
        }

	@Test
	public void testPE349() throws Throwable {
		StringBuilder buf = new StringBuilder();

		buf.append("SELECT 1+1,1-1,1+1*2,8/5,8%5,mod(8,5),mod(8,5)|0,-(1+1)*-2");
		conn.assertResults(buf.toString(), br(nr, 2L, 0L, 3L, new BigDecimal("1.6000"),
				3L, 3L, new Long("3"), 4L));

		buf = new StringBuilder();
		buf.append("CREATE TABLE `pe349` (`value` int(11) NOT NULL) ");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO pe349 VALUES (0),(1),(2),(3),(-1)");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT value FROM pe349 ORDER BY 1");
		conn.assertResults(buf.toString(), br(nr, -1, nr, 0, nr, 1, nr, 2, nr, 3));

		buf = new StringBuilder();
		buf.append("SELECT -value FROM pe349 ORDER BY 1");
		conn.assertResults(buf.toString(), br(nr, -3L, nr, -2L, nr, -1L, nr, 0L, nr, 1L));

		buf = new StringBuilder();
		buf.append("SELECT -ABS(-5) + -ABS(-12)");
		conn.assertResults(buf.toString(), br(nr, -17L));
	}

	@Test
	public void testPE823() throws Throwable {
		StringBuilder buf = new StringBuilder();

		buf.append("CREATE TABLE `pe823` (`value` int(11) NOT NULL) ");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO pe823 VALUES (0),(1),(2),(3),(-1)");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT * FROM pe823 WHERE value IS TRUE ORDER BY VALUE");
		conn.assertResults(buf.toString(), br(nr, -1, nr, 1, nr, 2, nr, 3));

		buf = new StringBuilder();
		buf.append("SELECT * FROM pe823 WHERE value IS NOT TRUE");
		conn.assertResults(buf.toString(), br(nr, 0));

		buf = new StringBuilder();
		buf.append("SELECT * FROM pe823 WHERE value IS FALSE");
		conn.assertResults(buf.toString(), br(nr, 0));

		buf = new StringBuilder();
		buf.append("SELECT * FROM pe823 WHERE value IS NOT FALSE ORDER BY VALUE");
		conn.assertResults(buf.toString(), br(nr, -1, nr, 1, nr, 2, nr, 3));
	}

	@Test
	public void testPE825() throws Throwable {
		StringBuilder buf = new StringBuilder();

		buf.append("CREATE TABLE `pe825` (`value1` int(11) NOT NULL, value2 int) ");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO pe825 VALUES (0,1),(1,1),(2,-1),(3,0),(-1,5)");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT value1 FROM pe825 ORDER BY 1");
		conn.assertResults(buf.toString(), br(nr, -1, nr, 0, nr, 1, nr, 2, nr, 3));

		buf = new StringBuilder();
		buf.append("SELECT value1, value2 FROM pe825 ORDER BY 2, 1");
		conn.assertResults(buf.toString(), br(nr, 2, -1, nr, 3, 0, nr, 0, 1, nr, 1, 1, nr, -1, 5));
	}

	@Test
	public void testPE864() throws Throwable {
		StringBuilder buf = new StringBuilder();

		buf.append("CREATE TABLE `PE864` (`value1` int(11) NOT NULL, value2 int) ");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO `PE864` VALUES (0,1),(1,1),(2,-1),(3,0),(-1,5)");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT * FROM `PE864` ORDER BY value1");
		conn.assertResults(buf.toString(), br(nr, -1, 5, nr, 0, 1, nr, 1, 1, nr, 2, -1, nr, 3, 0));

		buf = new StringBuilder();
		buf.append("SELECT * FROM (`PE864`) ORDER BY value1");
		conn.assertResults(buf.toString(), br(nr, -1, 5, nr, 0, 1, nr, 1, 1, nr, 2, -1, nr, 3, 0));
	}

	@Test
	public void testPE1008_SelectFromPlugins() throws Throwable {
		StringBuilder buf = new StringBuilder();

		buf = new StringBuilder();
		buf.append("SELECT * FROM INFORMATION_SCHEMA.PLUGINS");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("USE INFORMATION_SCHEMA");
		conn.execute(buf.toString());
		buf = new StringBuilder();
		buf.append("SELECT * FROM PLUGINS");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS");
		conn.execute(buf.toString());
	}

	@Test
	public void testPE1031() throws Throwable {
		String[] decls = {
				"CREATE TABLE IF NOT EXISTS `node` (`nid` int(10) unsigned NOT NULL)",
				"CREATE TABLE IF NOT EXISTS `fdfs` (`ei` int(10) unsigned NOT NULL, `et` varchar(128) NOT NULL DEFAULT '', `del` tinyint(4), `fsv` datetime)",
				"CREATE TABLE IF NOT EXISTS `cbe` (`pid` int(10) unsigned NOT NULL, `type` varchar(128) NOT NULL DEFAULT '')",
				"CREATE TABLE IF NOT EXISTS `fdfer` (`et` varchar(128) NOT NULL DEFAULT '', `bdl` varchar(128) NOT NULL DEFAULT '', `del` tinyint(4) NOT NULL DEFAULT '0', `ei` int(10) unsigned NOT NULL, `ri` int(10) unsigned DEFAULT NULL, `lg` varchar(32) NOT NULL DEFAULT '', `dl` int(10) unsigned NOT NULL, `ferti` int(10) unsigned NOT NULL,   PRIMARY KEY (`et`,`ei`,`del`,`dl`,`lg`),   KEY `et` (`et`),   KEY `bdl` (`bdl`),   KEY `del` (`del`),   KEY `ei` (`ei`),   KEY `ri` (`ri`),   KEY `lg` (`lg`),   KEY `ferti` (`ferti`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 RANDOM DISTRIBUTE",
				"CREATE TABLE IF NOT EXISTS `fdfsr` (`et` varchar(128) NOT NULL DEFAULT '', `bdl` varchar(128) NOT NULL DEFAULT '', `del` tinyint(4) NOT NULL DEFAULT '0', `ei` int(10) unsigned NOT NULL, `ri` int(10) unsigned DEFAULT NULL, `lg` varchar(32) NOT NULL DEFAULT '', `dl` int(10) unsigned NOT NULL, `fsrti` int(10) unsigned NOT NULL,   PRIMARY KEY (`et`,`ei`,`del`,`dl`,`lg`),   KEY `et` (`et`),   KEY `bdl` (`bdl`),   KEY `del` (`del`),   KEY `ei` (`ei`),   KEY `ri` (`ri`),   KEY `lg` (`lg`),   KEY `fsrti` (`fsrti`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 RANDOM DISTRIBUTE"
		};
		
		for(String decl : decls) {
			conn.execute(decl);
		}

		conn.execute("SELECT cbe.pid AS pid, fdfs.fsv AS fdfs_fsv, 'cbe' AS fdfs_cbe_et, 'cbe' AS fdfer_cbe_entity_ty, 'cbe' AS field_data_field_channel_cbe_et FROM cbe cbe LEFT JOIN fdfer fdfer ON cbe.pid = fdfer.ei AND (fdfer.et = 'cbe' AND fdfer.del = '0') INNER JOIN node node_fdfer ON fdfer.ferti = node_fdfer.nid LEFT JOIN fdfsr node_fdfer__fdfsr ON node_fdfer.nid = node_fdfer__fdfsr.ei AND (node_fdfer__fdfsr.et = 'node' AND node_fdfer__fdfsr.del = '0') INNER JOIN node node_fdfsr ON node_fdfer__fdfsr.fsrti = node_fdfsr.nid LEFT JOIN fdfs fdfs ON cbe.pid = fdfs.ei AND (fdfs.et = 'cbe' AND fdfs.del = '0') WHERE (( (node_fdfer__fdfsr.fsrti = '164180' ) )AND(( (cbe.type IN ('broadcast')) AND (DATE_FORMAT(ADDTIME(FROM_UNIXTIME(fdfs.fsv), SEC_TO_TIME(10800)), '%Y-%m-%d\\T%H:%i:%s') >= '2013-08-12T22:56:49') ))) ORDER BY fdfs_fsv ASC LIMIT 1 OFFSET 0 ");
	}
	
	
	@Test
    public void testPE983() throws Throwable {
		String[] decls = {
				"CREATE TABLE IF NOT EXISTS `b` (`id` int(10) unsigned NOT NULL)",
				"INSERT INTO `b` values (1)"
		};

		for(String decl : decls) {
			conn.execute(decl);
		}

		conn.assertResults("SELECT COUNT(1) FROM (SELECT `id` from `b`) a", br(nr,Long.valueOf(1)));
    }

	@Test
    public void testPE1232() throws Throwable {
		String[] decls = {
				"DROP TABLE IF EXISTS `test1232`",
				"CREATE TABLE `test1232` (`col1` bigint(22) unsigned)",
				"INSERT INTO `test1232` values (1),(2),(3)"
		};
		for(String decl : decls) {
			conn.execute(decl);
		}

		conn.assertResults("SELECT SUM(col1) as test1 FROM test1232 ORDER BY test1", br(nr,BigDecimal.valueOf(6)));
	}

	@Test
    public void testPE1235() throws Throwable {
		String[] decls = {
				"DROP TABLE IF EXISTS `test1235`",
				"CREATE TABLE `test1235` (`serial` int, col1 int, col2 int)",
				"INSERT INTO `test1235` values (1,3,3),(2,2,2),(3,1,1)"
		};
		for(String decl : decls) {
			conn.execute(decl);
		}

		conn.assertResults("SELECT serial as SERIAL FROM test1235 order by serial",
						br(nr, Integer.valueOf(1), nr,  Integer.valueOf(2), nr, Integer.valueOf(3)));

		// use this to repro PE-1243
		conn.assertResults("SELECT avg(col1) as AVG, max(col2) as MAX, min(col1) as MIN, sum(col2) as SUM FROM test1235",
				br(nr, BigDecimal.valueOf(20000, 4), Integer.valueOf(3), Integer.valueOf(1), BigDecimal.valueOf(6)));
	}
	
	@Test
	public void testPE1259() throws Throwable {
		conn.execute("create table locktest (id int, fid int, primary key (id)) random distribute");
		conn.execute("start transaction");
		conn.execute("select count(*) from locktest");
		conn.assertResults("show multitenant lock",
				br(
                    nr,"PE.Model.Range",ignore,"acquired","SHARED","[SHARED]",1,0,"default statement generation lock",
                    nr,"checkdb/locktest",ignore,"acquired","SHARED","[SHARED]",1,0,"select"
                )
        );
		conn.execute("commit");
		conn.execute("start transaction");
		conn.execute("select count(*) from locktest");
        conn.assertResults("show multitenant lock",
                br(
                        nr,"PE.Model.Range",ignore,"acquired","SHARED","[SHARED]",1,0,"default statement generation lock",
                        nr,"checkdb/locktest",ignore,"acquired","SHARED","[SHARED]",1,0,"plan cache hit"
                )
        );
		conn.execute("commit");		
		conn.assertResults("show multitenant lock",
                br(
                        nr,"PE.Model.Range",ignore,"acquired","SHARED","[SHARED]",1,0,"default statement generation lock"
                )
        );
	}

	@Test
	public void testPE361() throws Throwable {
		conn.execute("CREATE TABLE pe361_A (id INT)");
		conn.execute("CREATE TABLE pe361_B (id INT)");
		conn.execute("INSERT INTO pe361_A VALUES (1), (2), (3), (4), (5)");
		conn.execute("INSERT INTO pe361_B VALUES (1), (2), (3), (4), (5)");

		conn.assertResults("SELECT (1, 1) IN ((1, 1))", br(nr, 1l));
		conn.assertResults("SELECT (1, 2) IN ((1, 1), (1, 2), (2, 1), (2, 2))", br(nr, 1l));
		conn.assertResults("SELECT ((1, 2)) IN ((1, 1), (1, 2), (2, 1), (2, 2))", br(nr, 1l));
		conn.assertResults("SELECT ((1, 1), (1, 2)) IN (((1, 1), (1, 2)), ((2, 1), (2, 2)))", br(nr, 1l));
		conn.assertResults("SELECT ((1, 2), (2, 1)) IN (((1, 1), (1, 2)), ((2, 1), (2, 2)))", br(nr, 0l));

		conn.assertResults("SELECT 1 IN (1,2,3)", br(nr, 1l));
		conn.assertResults("SELECT (1,2,3) IN ((3,2,3), (1,2,3), (1,3,3))", br(nr, 1l));

		conn.assertResults("SELECT * FROM pe361_A a INNER JOIN pe361_B b WHERE (a.id, b.id) IN ((1,1), (4,5), (2,1)) ORDER BY a.id ASC;",
				br(nr, 1, 1, nr, 2, 1, nr, 4, 5));
		conn.assertResults("SELECT * FROM pe361_A a INNER JOIN pe361_B b WHERE (a.id, b.id) IN ((7,1), (2,8), (4,-4)) ORDER BY a.id ASC;", br());
		conn.assertResults(
				"SELECT * FROM pe361_A a INNER JOIN pe361_B b WHERE (a.id, b.id) IN ((1 + 1, 2 - 1), ((2 + 1), (8 - 5)), (1 = 1, (0 <> 1))) ORDER BY a.id ASC;",
				br(nr, 1, 1, nr, 2, 1, nr, 3, 3));
	}
	
	// testing performance of 'use'
	public static void testUsePerformance(ConnectionResource cr, String[] dbs) throws Throwable {
		for(String s : dbs) {
			cr.execute("use " + s);
			cr.execute("create table tup (id int, `what` varchar(32), primary key (id))");
			cr.execute("insert into tup (id, what) values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");
		}
		for(int i = 0; i < 1000; i++) {
			for(String s : dbs) {
				cr.execute("use " + s);
				cr.fetch("select * from tup");
			}
		}
	}
	
	@Test
	public void testPE1163() throws Throwable {
		otherDDL.create(conn);
		try {
			testUsePerformance(conn, new String[] { checkDDL.getDatabaseName(), otherDDL.getDatabaseName() });
		} finally {
			otherDDL.destroy(conn);
		}
	}
}
