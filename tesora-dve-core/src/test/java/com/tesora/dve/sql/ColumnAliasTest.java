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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class ColumnAliasTest extends SchemaTest {

	private static final StorageGroupDDL theStorageGroup = 
		new StorageGroupDDL("check",3,"checkg");

	private static final ProjectDDL testDDL =
		new PEDDL("checkdb",theStorageGroup,"schema");

	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		testDDL.create(pcr);
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
		if(conn != null)
			conn.disconnect();
		conn = null;
	}

	@Test
	public void testWhereClauseReferenceAlias() throws Throwable {
		String PERange = "node_range";
		String[] decls = new String[] {
				"CREATE TABLE stti (`nid` INT unsigned NOT NULL DEFAULT 0 , `tid` INT unsigned NOT NULL DEFAULT 0, `sticky` TINYINT NULL DEFAULT 0 , `created` INT NOT NULL DEFAULT 0 ,  INDEX `term_node` ( `tid`, `sticky`, `created` ) ,  INDEX `nid` ( `nid` ) ) ENGINE = InnoDB  CHARSET utf8  RANDOM DISTRIBUTE",
				"CREATE TABLE stna (`nid` INT unsigned NOT NULL DEFAULT 0 , `gid` INT unsigned NOT NULL DEFAULT 0, `realm` VARCHAR (255) NOT NULL DEFAULT '', `grant_view` TINYINT unsigned NOT NULL DEFAULT 0, `grant_update` TINYINT unsigned NOT NULL DEFAULT 0, `grant_delete` TINYINT unsigned NOT NULL DEFAULT 0, PRIMARY KEY ( `nid`, `gid`, `realm` ) ) ENGINE = InnoDB  CHARSET utf8  RANGE DISTRIBUTE ON (`nid`) USING " + PERange
		};
		conn.execute("use checkdb");
		conn.execute("create range " + PERange + " (INT) persistent group " + testDDL.getPersistentGroup().getName());
		conn.execute(decls[0]);
		conn.execute(decls[1]);

		conn.execute("INSERT INTO stti (nid, tid, sticky, created) VALUES ('1', '1', '0', '1324417805')");
		conn.execute("INSERT INTO stti (nid, tid, sticky, created) VALUES ('2', '2', '0', '1324417806')");
		conn.execute("INSERT INTO stti (nid, tid, sticky, created) VALUES ('3', '1', '0', '1324417807')");
		conn.execute("INSERT INTO stti (nid, tid, sticky, created) VALUES ('4', '2', '0', '1324417807')");
		conn.execute("INSERT INTO stna (nid, gid, realm, grant_view, grant_update, grant_delete) VALUES ('0', '0', 'all', '1', '0', '0')");
		conn.execute("INSERT INTO stna (nid, realm, gid, grant_view, grant_update, grant_delete) VALUES ('1', 'all', '0', '1', '0', '0')");
		conn.execute("INSERT INTO stna (nid, realm, gid, grant_view, grant_update, grant_delete) VALUES ('2', 'node_access_test', '8888', '1', '0', '0'), ('2', 'node_access_test_author', '2', '1', '1', '1')");
		conn.execute("INSERT INTO stna (nid, realm, gid, grant_view, grant_update, grant_delete) VALUES ('3', 'all', '0', '1', '0', '0')");
		conn.execute("INSERT INTO stna (nid, realm, gid, grant_view, grant_update, grant_delete) VALUES ('4', 'node_access_test', '8888', '1', '0', '0'), ('4', 'node_access_test_author', '3', '1', '1', '1')");

		conn.assertResults("SELECT DISTINCT t.nid AS nid,t.tid AS tid,t.sticky AS sticky,t.created AS created FROM `stti` AS t INNER JOIN `stna` AS na ON na.nid = t.nid WHERE  (tid = '1')  AND  ( ( (na.gid = '0')  AND  (na.realm = 'all') )  OR  ( (na.gid = '2')  AND  (na.realm = 'node_access_test_author') ) )  AND  (na.grant_view >= '1')  ORDER BY sticky DESC, created DESC LIMIT 0, 10",
				br(nr, Long.valueOf(3), Long.valueOf(1), Byte.valueOf((byte) 0), Integer.valueOf(1324417807),
				   nr, Long.valueOf(1), Long.valueOf(1), Byte.valueOf((byte) 0), Integer.valueOf(1324417805)));
	}
	
	@Test
	public void testPE667() throws Throwable {
		String[] decls = new String[] {
				"CREATE TABLE pe667 (`nid` INT, `uuid` INT)"
		};
		conn.execute("use checkdb");
		conn.execute(decls[0]);
		conn.execute("INSERT INTO pe667 (nid, uuid) VALUES (1, 1)");
		conn.execute("INSERT INTO pe667 (uuid, nid) VALUES (2, 2)");
		conn.execute("INSERT INTO pe667 (uuid) VALUES (3)");
		conn.assertResults("SELECT pe667.nid as nid, pe667.uuid as uuid from pe667 order by uuid", 
				br(nr,1,1,nr,2,2,nr,null,3));
	}
	
}
