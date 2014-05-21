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

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class TextPrepareTest extends SchemaTest {

	private static StorageGroupDDL sg = new StorageGroupDDL("sys",5, 2,"sysg");
	 
	private static final ProjectDDL sysDDL =
			new PEDDL("adt",sg,
					"database").withFKMode(FKMode.IGNORE);
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(sysDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		PortalDBHelperConnectionResource pcr = new PortalDBHelperConnectionResource();
		sysDDL.create(pcr);
		pcr.disconnect();
	}
	
	PortalDBHelperConnectionResource conn = null;
	
	@Before
	public void begin() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
	}
	
	@After
	public void after() throws Throwable {
		conn.disconnect();
		conn = null;
	}
	
	@Test
	public void testPE1333() throws Throwable {
		conn.execute("use " + sysDDL.getDatabaseName());
		conn.execute("prepare foo from 'set names utf8 collate utf8_unicode_ci'");
		conn.execute("execute foo");
		conn.execute("deallocate prepare foo");
	}
	
	@Test
	public void testNoCurrentDB() throws Throwable {
		conn.execute("prepare foo from 'set names utf8 collate utf8_unicode_ci'");
		conn.execute("execute foo");
		conn.execute("deallocate prepare foo");		
	}
	
	@Test
	public void testBasicSubstitution() throws Throwable {
		conn.execute("use " + sysDDL.getDatabaseName());		
		conn.execute("create table atab (`id` int, `what` varchar(32), primary key (id)) random distribute");
		conn.execute("prepare istmt from 'insert into atab (id,what) values (?,?),(?,?)'");
		conn.execute("set @i1 = 1, @w1 = 'one', @i2 = 2, @w2 = 'two'");
		conn.execute("execute istmt using @i1,  @w1, @i2, @w2");
		conn.assertResults("select what from atab order by id",br(nr,"one",nr,"two"));
		conn.execute("prepare vstmt from 'select what from atab where id = ?'");
		conn.assertResults("execute vstmt using @i1", br(nr,"one"));
		conn.execute("set @i3 = 3, @w3 = 'three', @i4 = 4, @w4 = 'four'");
		conn.execute("execute istmt using @i3, @w3, @i4, @w4");
		conn.assertResults("execute vstmt using @i3",br(nr,"three"));
		conn.execute("deallocate prepare istmt");
		conn.execute("deallocate prepare vstmt");
	}
	
	@Test
	public void testPE1391() throws Throwable {
		conn.execute("use " + sysDDL.getDatabaseName());
		conn.execute("create table tab1 (id int, fid int, primary key (id)) broadcast distribute");
		conn.execute("create table tab2 (id int, fid int, primary key (id)) broadcast distribute");
		conn.execute("prepare pe1391 from 'select t1.fid, t2.id from tab1 t1 join tab2 t2 on t1.id = t2.id union (select t1.fid, t2.id from tab1 t1 left outer join tab2 t2 on t1.id = t2.id) order by fid desc limit 10, 0'");
	}
	
}
