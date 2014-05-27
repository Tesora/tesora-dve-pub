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

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.util.ComparisonOptions;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class ViewTest extends SchemaTest {

	private static final ProjectDDL checkDDL =
			new PEDDL("adb",
					new StorageGroupDDL("check",3,"checkg"),
					"database");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	private PortalDBHelperConnectionResource conn;
	private PortalDBHelperConnectionResource rconn;

	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		rconn = new PortalDBHelperConnectionResource();
	}

	@After
	public void after() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
		if (rconn != null)
			rconn.disconnect();
		rconn = null;
	}


	
    @Test
    public void testPassthroughView() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		rconn.execute("use " + checkDDL.getDatabaseName());
    		conn.execute("create table A (id int, primary key (id)) broadcast distribute");
    		conn.execute("create table B (id int, what varchar(32), primary key (id)) broadcast distribute");
    		conn.execute("create table C (id int, sig varchar(32), primary key (id)) random distribute");
    		conn.execute("create view AB as select a.id, b.what from A a inner join B b on a.id = b.id");
    		conn.execute("create view BC as select b.id, c.sig from B b inner join C c on b.id = c.id");
    		conn.execute("insert into A (id) values (1),(2),(3)");
    		conn.execute("insert into B (id, what) values (1,'one'),(2,'two'),(3,'three')");
    		conn.execute("insert into C (id, sig) values (1,'eno'),(2,'owt'),(3,'eerth')");
    		viewTest(false,"select a.id, b.what from A a inner join B b on a.id = b.id","select * from AB");
    		viewTest(true,"select b.id, c.sig from B b inner join C c on b.id = c.id order by id","select * from BC order by id");
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }

    protected void viewTest(boolean ignoreOrder, String plain, String view) throws Throwable {
		ResourceResponse plainResponse = conn.fetch(plain);
		ResourceResponse viewResponse = rconn.fetch(view);
		ComparisonOptions options = ComparisonOptions.DEFAULT.withIgnoreMD();
		if (ignoreOrder)
			options = options.withIgnoreOrder();
		plainResponse.assertEqualResults("", viewResponse, options);    	
    }
 
    @Test
    public void testNonUpdatableView() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		conn.execute("create table A (id int, fid int, primary key (id)) broadcast distribute");
    		conn.execute("create table B (id int, fid int, wid int, primary key (id)) broadcast distribute");
    		conn.execute("create view AB as select b.id as f, a.fid as s, b.wid as t from A a inner join B b on a.id = b.id");
    		String ok[] = new String[] {
    				"select * from AB",
    				"select * from AB order by t",
    				"select ab.t from AB ab inner join A a on ab.f = a.id",
    				"insert into A (id,fid) select ab.f, ab.s from AB ab order by ab.t"
    		};
    		String notok[] = new String[] {
    				"insert into AB (f,s,t) values (1,1,1)",
    				"delete from AB where f = 22",
    				"update AB set f = 31 where t > 15",
    				"insert into AB (f,s,t) select a.id, a.id, a.id from A a where a.fid > 15"
    		};
    		for(String s : ok) 
    			conn.execute(s);
    		// none of these should work
    		for(String s : notok) {
    			try {
    				conn.execute(s);
    				fail("Should not be able to execute '" + s + "'");
    			} catch (Throwable t) {
    				String m = t.getMessage();
    				if (m.indexOf("No support for updatable views") > -1) {
    					// ok	
    				} else {
    					System.err.println(s);
        				t.printStackTrace();
        				throw t;
    				}
    			}
    		}
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }
    
    @Test
    public void testPE1282() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		conn.execute("create table A (id int, fid int, primary key (id)) broadcast distribute");
    		conn.execute("create view VA as select id + 22 as id, fid * 100 as fid from A");
    		SchemaSourceFactory.reset();
    		conn.execute("select * from VA limit 1");
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }

}
