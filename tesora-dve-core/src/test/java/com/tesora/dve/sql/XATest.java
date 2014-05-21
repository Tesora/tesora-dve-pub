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

public class XATest extends SchemaTest {


	private static final ProjectDDL testDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",3,"checkg"),
				"schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected ProxyConnectionResource conn;
	
	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
		testDDL.create(conn);
	}
	
	@After
	public void after() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
	}

	@Test
	public void testXATranSimple() throws Throwable {
		conn.execute("create table xasimple (id int, fid varchar(32), primary key (id)) broadcast distribute");
		conn.execute("xa start 'mytrans1'");
		conn.execute("insert into xasimple (id, fid) values (1,'1'),(2,'2'),(3,'3')");
		conn.execute("xa end 'mytrans1'"); // noop
		conn.execute("xa commit 'mytrans1'");
		conn.assertResults("select * from xasimple order by id",
				br(nr,new Integer(1),"1",
				   nr,new Integer(2),"2",
				   nr,new Integer(3),"3"));
	}
	
	@Test
	public void testXATranRollback() throws Throwable {
		String thatWierdTransID = "0xfc6d190044d398be646564693032302d3139382e6272616e6470726f74656374696f6e2e6e65742c7365727665722c5033373030,0x646564693032302d3139382e6272616e6470726f74656374696f6e2e6e65742c7365727665722c50333730302c00,0x4a5453";
		conn.execute("create table xahollaback (id int, fid varchar(32), primary key (id)) random distribute");
		conn.execute("insert into xahollaback (id,fid) values (1,'ohmy'),(2,'wowsas'),(3,'whatevs')");
		conn.execute("xa start " + thatWierdTransID);
		conn.assertResults("select count(*) from xahollaback",br(nr,new Long(3)));
		conn.execute("insert into xahollaback (id,fid) values (4,'lost'),(5,'to'),(6,'the ages')");
		conn.assertResults("select count(*) from xahollaback",br(nr,new Long(6)));
		conn.execute("xa rollback " + thatWierdTransID);
		conn.assertResults("select count(*) from xahollaback",br(nr,new Long(3)));		
	}
	
	@Test
	public void testXARecover() throws Throwable {
		conn.execute("create table xareco(id int, fid varchar(32), primary key (id)) random distribute");
		conn.execute("xa start 'mytrans2'");
		conn.execute("xa end 'mytrans2'");
		// doesn't work yet, but when it does, line 'em up
		conn.assertResults("xa recover",br());
		conn.execute("xa commit 'mytrans2' one phase");
		conn.assertResults("xa recover",br());		
	}
}
