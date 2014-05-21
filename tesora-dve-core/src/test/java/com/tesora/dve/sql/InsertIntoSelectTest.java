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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ProxyConnectionResourceResponse;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class InsertIntoSelectTest extends SchemaTest {

	private static final StorageGroupDDL theStorageGroup = 
		new StorageGroupDDL("check",3,"checkg");

	private static final StorageGroupDDL otherStorageGroup = 
		new StorageGroupDDL("check1",1,"checkg1");
	
	private static final ProjectDDL testDDL =
		new PEDDL("checkdb",theStorageGroup,"schema");

	private static final ProjectDDL otherDDL =
		new PEDDL("otherdb",otherStorageGroup,"database");
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(testDDL, otherDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		testDDL.create(pcr);
		otherDDL.create(pcr);
		pcr.disconnect();
		pcr = null;
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
	public void testInsertIntoSelectMultiDB() throws Throwable {
		String[] decls = new String[] {
				"create table tab (`id` int, `stuff` varchar(32)) random distribute",
				"create table shelf (`id` int, `stuff` varchar(32)) random distribute"
		};
		conn.execute("use otherdb");
		conn.execute(decls[0]);
		conn.execute(decls[1]);
		conn.execute("insert into tab values (1,'one'), (2,'two'), (3,'three')");

		conn.execute("use checkdb");
		conn.execute(decls[1]);
		conn.execute("insert into shelf select * from otherdb.tab");
		conn.assertResults("select * from shelf order by `id`",
				br(nr,1,"one",nr,2,"two",nr,3,"three"));
	}

	// regression test for PE-223
	@Test
	public void testInsertIntoSelectBroadcast() throws Throwable {
		String[] decls = new String[] {
				"create table tblin (`id` int, `stuff` varchar(32)) broadcast distribute",
				"create table tblout (`id` int, `stuff` varchar(32)) broadcast distribute"
		};
		conn.execute("use otherdb");
		conn.execute(decls[0]);
		conn.execute("insert into tblin values (1,'one'), (2,'two'), (3,'three')");

		conn.execute("use checkdb");
		conn.execute(decls[1]);
		ProxyConnectionResourceResponse pcr = (ProxyConnectionResourceResponse) conn.execute("insert into tblout select * from otherdb.tblin");
		assertEquals(3, pcr.getNumRowsAffected());
	}
	
	// regression test for PE-238
	@Test
	public void testInsertIntoSelectFromSelf() throws Throwable {
		String[] decls = new String[] {
				"create table self (`id` int) broadcast distribute"
		};

		conn.execute("use checkdb");
		conn.execute(decls[0]);
		for ( int i = 0; i < 101; i++ ) {
			conn.execute("insert into self values (" + i + ")");
		}
		
		conn.execute("insert into self select id+1 from self");
	}
	
	@Test
	public void testInsertIntoSelectFromSelfwithAI() throws Throwable {
		String prefix = "self_ai";
		String colDecl = " (`id` int auto_increment primary key, `col1` int) "; 
		String[] tabNames = new String[] { prefix+"B", prefix+"A", prefix+"R" };
		
		String[] decls = new String[] {
				"create range " + prefix +"_range (int) persistent group " + testDDL.getPersistentGroup().getName(),
				"create table " + tabNames[0] + colDecl + "broadcast distribute",
				"create table " + tabNames[1] + colDecl + "random distribute",
				"create table " + tabNames[2] + colDecl + "range distribute on (`col1`) using " + prefix + "_range"
		};

		conn.execute("use checkdb");
		for (int i = 0; i < decls.length; i++) {
			conn.execute(decls[i]);
		}

		for ( int j = 0; j < tabNames.length; j++ ) {
			for ( int i = 0; i < 101; i++ ) {
				conn.execute("insert into " + tabNames[j] + " (col1) values (" + i + ")");
			}
		
			conn.execute("insert into " + tabNames[j] + " (col1, id) select col1+1, id+101 from " + tabNames[j]);
			conn.execute("insert into " + tabNames[j] + " (col1) values (1000)");
			conn.assertResults("select count(*) from " + tabNames[j], br(nr,Long.valueOf(203L)));
			
			try {
				conn.execute("insert into " + tabNames[j] + " (id, col1) select NULL, col1 from " + tabNames[j] + " where id<10");
			} catch (PEException e) {
				assertException(e, PEException.class, "Found NULL value for auto-increment column in table " + tabNames[j]);
			}
		}
	}

	@Test
	public void test_PE235() throws Throwable {
		String[] decls = new String[] {
				"create table PE235tblin (`id` int, `stuff` varchar(32)) broadcast distribute",
				"create table PE235tblout (`id` int, `stuff` varchar(32)) broadcast distribute"
		};

		conn.execute("use checkdb");
		conn.execute(decls[0]);
		conn.execute(decls[1]);
		ProxyConnectionResourceResponse pcr = (ProxyConnectionResourceResponse) conn.execute("insert into PE235tblin values (1,'one'), (2,'two'), (3,'three')");
		assertEquals(3, pcr.getNumRowsAffected() );
		conn.execute("select id as id from PE235tblin");
		
		pcr = (ProxyConnectionResourceResponse) conn.execute("insert into PE235tblout select id as id, stuff as stuff from PE235tblin");
		assertEquals(3, pcr.getNumRowsAffected());
	}

	// regression test for PE-242
	@Test
	public void testInsertIntoSelectFromSelfRandom() throws Throwable {
		String[] decls = new String[] {
				"create table selfrand (`id` int)"
		};

		conn.execute("use checkdb");
		conn.execute(decls[0]);
		for ( int i = 0; i < 100; i++ ) {
			conn.execute("insert into selfrand values (" + i + ")");
		}
		
		conn.execute("insert into selfrand select id+1 from selfrand");
	}

	@Test
	public void testPE244_SelectFromBinaryRangeDist() throws Throwable {
		String[] decls = new String[] {
				"create table PE244tblin (`id` int, `stuff` binary(36))",
				"create table PE244tblout (`id` int, `stuff` binary(36))"
		};
		// create the multidb table first
		String PERange = "PE244Range";
		conn.execute("use checkdb");
		conn.execute("create range " + PERange + " (binary(36) using 'com.tesora.dve.comparator.UUIDComparator') persistent group " + testDDL.getPersistentGroup().getName());
		conn.execute(decls[0] + " range distribute on (`stuff`) using " + PERange);
		conn.execute(decls[1] + " range distribute on (`stuff`) using " + PERange);

		// then create the single site
		conn.execute("use otherdb");
		conn.execute(decls[0] + "broadcast distribute");
		conn.execute(decls[1] + "broadcast distribute");

		// insert rows into the single site
		int totalRows = 0;
		for(int i = 0; i < 20; i++) {
			conn.execute("insert into PE244tblin values (" + i + ", UUID())");
			totalRows++;
		}

		// do the insert into select from single site to multi
		conn.execute("use checkdb");
		ProxyConnectionResourceResponse pcr = (ProxyConnectionResourceResponse) conn.execute("insert into PE244tblout select * from otherdb.PE244tblin");
		assertEquals(totalRows, pcr.getNumRowsAffected());

		// check for each range distributed value make sure we can do a select and we can find it from the right persistent site
		ResourceResponse rr =  conn.fetch("select distinct stuff from PE244tblout");
		Object o = null;
		for(ResultRow row : rr.getResults()) {
			ResultColumn col = row.getResultColumn(1);
			o = col.getColumnValue();

			ResourceResponse resp = conn.fetch("select count(*) from PE244tblout where stuff='"+UUID.fromString(new String((byte[])o))+"'");
			Long count = (Long)resp.getResults().get(0).getResultColumn(1).getColumnValue();
			assertTrue("Should find the row from the correct persistent site for a binary range dist column", count>0);
		}
	}

	@Test
	public void test_PE291() throws Throwable {
		String[] decls = new String[] {
				"create table PE291tbl (`id` int, `stuff` varchar(36))"
		};
		String PERange = "PE291Range";
		conn.execute("use checkdb");
		conn.execute("create range " + PERange + " (int) persistent group " + testDDL.getPersistentGroup().getName());
		conn.execute(decls[0] + " range distribute on (`id`) using " + PERange);

		// insert rows into the table
		for(int i = 0; i < 5; i++) {
			conn.execute("insert into PE291tbl values (" + i + ", '" + i + "')");
		}

		// test that id as a literal works
		conn.assertResults("select * from PE291tbl where `id` in ('3')",
				br(nr,3,"3"));
	}
	
	@Test
	public void testPE1330() throws Throwable {
		String[] decls = new String[] {
			"DROP TABLE IF EXISTS `ic_community`",
			"DROP TABLE IF EXISTS `slms_community`",
			"CREATE TABLE `ic_community` (`COMMUNITY_ID` varchar(32) NOT NULL DEFAULT '',`INSTALLATION_ID` varchar(32) NOT NULL DEFAULT '',`DOMAIN_ID` varchar(32) NOT NULL DEFAULT '',`ORGANIZATION_ID` varchar(32) NOT NULL DEFAULT '',`NAME` varchar(100) NOT NULL DEFAULT '',`DESCRIPTION` varchar(255) DEFAULT NULL,`PATH` varchar(100) NOT NULL DEFAULT '/',`INHERIT_STYLES` tinyint(4) NOT NULL DEFAULT '1',`ACTIVE_SKIN_ID` varchar(32) DEFAULT NULL,`LAUNCH_FRAME` tinyint(4) NOT NULL DEFAULT '0',`START_PAGE` varchar(100) NOT NULL DEFAULT '/LyceaLogin',`LOGIN_PAGE` varchar(100) NOT NULL DEFAULT '/LyceaLogin',`HUB_PAGE` varchar(100) NOT NULL DEFAULT '/apps/shell/frames.html',`USE_HTTP_SESSION` tinyint(4) NOT NULL DEFAULT '0',`LOG_LOGINS` tinyint(4) DEFAULT '1',`LOG_FAILED_LOGINS` tinyint(4) DEFAULT '0',`LOG_CLIENT_INFO` tinyint(4) DEFAULT '1',`SYSTEM_DELIVERED` tinyint(4) NOT NULL DEFAULT '0',`IS_ENABLED` tinyint(4) DEFAULT '1',`IS_DELETABLE` tinyint(4) DEFAULT '0',PRIMARY KEY (`COMMUNITY_ID`),  UNIQUE KEY `UK1_IC_COMMUNITY` (`NAME`,`ORGANIZATION_ID`),KEY `IC_COMMUNITY_IDX_01` (`INSTALLATION_ID`),KEY `IC_COMMUNITY_IDX_02` (`ORGANIZATION_ID`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */",
			"CREATE TABLE `slms_community` (`community_id` varchar(32) NOT NULL, `restrict_access` tinyint(1) DEFAULT '0', PRIMARY KEY (`community_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */",
			"INSERT INTO `ic_community` (`COMMUNITY_ID`) VALUES ('CDX')"
		};
		conn.execute("use checkdb");
		for(String s : decls)
			conn.execute(s);
		conn.execute("INSERT INTO slms_community(community_id, restrict_access) SELECT community_id,1 FROM ic_community WHERE community_id != 'SLMS' AND is_enabled = 1");
		conn.execute("INSERT IGNORE INTO slms_community(community_id, restrict_access) SELECT community_id,1 FROM ic_community WHERE community_id != 'SLMS' AND is_enabled = 1");
		try {
			conn.execute("INSERT INTO slms_community(community_id, restrict_access) SELECT community_id,1 FROM ic_community WHERE community_id != 'SLMS' AND is_enabled = 1");
		} catch (PEException e) {
			assertException(e, PESQLStateException.class, "(1062: 23000) Duplicate entry 'CDX' for key 'PRIMARY'");
		}
		conn.execute("INSERT IGNORE INTO slms_community(community_id, restrict_access) SELECT community_id,1 FROM ic_community WHERE community_id != 'SLMS' AND is_enabled = 1");
	}
}
