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
import org.junit.Ignore;
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
	
	// waiting for runtime support of insert and delete triggers
	// the test keeps an up-to-date table (`data_stats`) of sums of values stored in the `data_point` table.
	@Ignore
	@Test
	public void testB() throws Throwable {
		conn.execute("CREATE RANGE arange (int) PERSISTENT GROUP " + checkDDL.getPersistentGroup().getName());

		conn.execute("CREATE TABLE `data_point` (`id` int(11) NOT NULL AUTO_INCREMENT, `gid` int(11) NOT NULL, `value` double NOT NULL, PRIMARY KEY (`id`)) RANGE DISTRIBUTE ON (`id`) USING arange");
		conn.execute("CREATE TABLE `data_stats` (`gid` int(11) NOT NULL, `sum` double NOT NULL) RANGE DISTRIBUTE ON (`gid`) USING arange");

		//		conn.execute("CREATE TRIGGER `add_on_insert` AFTER INSERT ON `data_point` FOR EACH ROW BEGIN CASE (SELECT (COUNT(*) > 0) FROM `data_stats` WHERE `gid` = NEW.gid) WHEN FALSE THEN BEGIN INSERT INTO `data_stats` (`gid`, `sum`) VALUES (NEW.gid, 0) ; END; ELSE BEGIN UPDATE `data_stats` SET `sum` = `sum` + NEW.value WHERE `gid` = NEW.gid; END; END CASE; END;");
		conn.execute("CREATE TRIGGER `update_on_change` AFTER UPDATE ON `data_point` FOR EACH ROW BEGIN UPDATE `data_stats` SET `sum` = `sum` + (NEW.value - OLD.value) WHERE `gid` = OLD.gid; END;");
		//		conn.execute("CREATE TRIGGER `subtract_on_delete` AFTER DELETE ON `data_point` FOR EACH ROW BEGIN UPDATE `data_stats` SET `sum` = `sum` - OLD.value WHERE `gid` = OLD.gid; END;");

		conn.execute("INSERT INTO `data_point` (`gid`, `value`) VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0), (1, 5.5)");
		conn.execute("INSERT INTO `data_point` (`gid`, `value`) VALUES (2, 0.1), (2, 0.2), (2, 0.3), (2, 0.4), (2, 0.5)");

		//		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 1", br(nr, 15.5));
		//		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 2", br(nr, 1.5));

		conn.execute("UPDATE `data_point` SET `value` = 5.0 WHERE `id` = 5");

		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 1", br(nr, 15.0));
		//		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 2", br(nr, 1.5));

		//		conn.execute("DELETE FROM `data_stats` WHERE `id` = 3");

		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 1", br(nr, 12.0));
		//		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 2", br(nr, 1.5));
	}

}
