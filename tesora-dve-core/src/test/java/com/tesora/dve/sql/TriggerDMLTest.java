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
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class TriggerDMLTest extends SchemaTest {

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

	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		checkDDL.create(conn);
	}

	@After
	public void after() throws Throwable {
		checkDDL.destroy(conn);
		conn.close();
		conn = null;
	}

	@Test
	public void testA() throws Throwable {
		conn.execute("create range arange (int) persistent group " + checkDDL.getPersistentGroup().getName());
		conn.execute("create table tlog (id int, kind varchar(64), action varchar(8)) random distribute");
		conn.execute("create table subj (id int auto_increment, kind varchar(16), primary key (id)) range distribute on (id) using arange");
		conn.execute("insert into subj (kind) values ('direct'), ('indirect'), ('sideways'), ('backwards')");
		conn.execute("insert into tlog (id, kind) select s.id, s.kind from subj s");
		conn.execute("create trigger subj_del before delete on subj for each row begin update tlog set action = 'delete' where id = OLD.id; end");
		conn.execute("create trigger subj_upd after update on subj for each row begin update tlog set action = 'update', kind = concat(OLD.kind,',',NEW.kind) where id = OLD.id; end");
		conn.assertResults("select * from tlog order by id", 
				br(nr,1,"direct",null,
				   nr,2,"indirect",null,
				   nr,3,"sideways",null,
				   nr,4,"backwards",null));
		conn.execute("delete from subj where kind = 'backwards'");
		conn.assertResults("select * from tlog order by id",
				br(nr,1,"direct",null,
						nr,2,"indirect",null,
						nr,3,"sideways",null,
						nr,4,"backwards","delete"));
		conn.execute("update subj set kind = 'inverted' where kind = 'sideways'");
		conn.assertResults("select * from tlog order by id",
				br(nr,1,"direct",null,
						nr,2,"indirect",null,
						nr,3,"inverted,sideways","update",
						nr,4,"backwards","delete"));
		
	}
	
}
