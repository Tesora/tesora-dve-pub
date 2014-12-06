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

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.SchemaTest.StatementMirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
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
		conn.execute("alter dve set adaptive_cleanup_interval = 0");
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
						nr,3,"sideways,inverted","update",
						nr,4,"backwards","delete"));
		
	}
	
	@Test
	public void testB() throws Throwable {
		conn.execute("create range arange (int) persistent group " + checkDDL.getPersistentGroup().getName());
		conn.execute("create table ref (id int, subject varchar(64), primary key (id)) range distribute on (id) using arange");
		conn.execute("create table lookup (id int, kind int, targ int, primary key (id)) random distribute");
		conn.execute("create table subj (id int, action varchar(16), primary key (id)) range distribute on (id) using arange");
		conn.execute("insert into ref (id, subject) values (1,'math'),(3,'physics'),(4,'chemistry'),(5,'religion')");
		conn.execute("insert into lookup (id, kind, targ) values (1,5,1),(3,5,3),(4,4,4),(5,6,5)");
		conn.execute("insert into subj (id, action) values (1,'run'),(3,'jump'),(4,'hop'),(5,'stand')");
		
		conn.execute("create trigger subj_upd after update on subj for each row "
				+"begin update ref r inner join lookup l on r.id = l.targ set r.subject = concat(OLD.action,',',NEW.action) where l.kind = 5 ; end");
		
		conn.assertResults("select * from ref order by id", 
				br(nr,1,"math",
				   nr,3,"physics",
				   nr,4,"chemistry",
				   nr,5,"religion"));
		conn.assertResults("select * from subj order by id", 
				br(nr,1,"run",
				   nr,3,"jump",
				   nr,4,"hop",
				   nr,5,"stand"));
		
		conn.execute("update subj s inner join lookup l on s.id = l.targ set s.action = 'firsttest' where l.kind = 5");

		conn.assertResults("select * from ref order by id", 
				br(nr,1,"jump,firsttest",
				   nr,3,"jump,firsttest",
				   nr,4,"chemistry",
				   nr,5,"religion"));

		conn.assertResults("select * from subj order by id",
				br(nr,1,"firsttest",
				   nr,3,"firsttest",
				   nr,4,"hop",
				   nr,5,"stand"));
	}

	// I know it seems dumb, but the point of this was verify that we get the correct allocated autoinc values
	@Test
	public void testC() throws Throwable {
		conn.execute("create range arange (int) persistent group " + checkDDL.getPersistentGroup().getName());
		conn.execute("create table alog (id int, subj varchar(32), primary key (id)) range distribute on (id) using arange");
		conn.execute("create table targ (id int auto_increment, subj varchar(32), primary key (id)) range distribute on (id) using arange");
		conn.execute("create trigger targ_ins after insert on targ for each row "
				+"begin insert into alog (id, subj) values (NEW.id, NEW.subj); end");
		
		conn.execute("insert into targ (subj) values ('boom!')");
		
		conn.assertResults("select * from targ where id = 1",
				br(nr,1,"boom!"));
		conn.assertResults("select * from alog where id = 1",
				br(nr,1,"boom!"));
		
		conn.execute("insert into targ (subj) values ('hi'),('hello'),('howdy'),('hiya')");

		Object[] results = br(nr,1,"boom!",
				nr,2,"hi",
				nr,3,"hello",
				nr,4,"howdy",
				nr,5,"hiya");
		
		conn.assertResults("select * from targ order by id", results);
		conn.assertResults("select * from alog order by id", results);

		// make sure that alog & targ are colocated
		List<ResultRow> alogResults = conn.execute("select id, cast(@dve_sitename as char(16)) from alog order by id").getResults(); 
		ResourceResponse targLocs = conn.execute("select id, cast(@dve_sitename as char(16)) from targ order by id");

		targLocs.assertResultsEqual("colocation test", targLocs.getResults(), alogResults, targLocs.getColumnCheckers());
				
	}
	
	
	// waiting for runtime support of insert and delete triggers
	// the test keeps an up-to-date table (`data_stats`) of sums of values stored in the `data_point` table.
    @Test
    public void testD() throws Throwable {
		conn.execute("CREATE RANGE arange (int) PERSISTENT GROUP " + checkDDL.getPersistentGroup().getName());

		conn.execute("CREATE TABLE `data_point` (`id` int(11) NOT NULL AUTO_INCREMENT, `gid` int(11) NOT NULL, `value` double NOT NULL, PRIMARY KEY (`id`)) RANGE DISTRIBUTE ON (`id`) USING arange");
		conn.execute("CREATE TABLE `data_stats` (`gid` int(11) NOT NULL, `sum` double NOT NULL) RANGE DISTRIBUTE ON (`gid`) USING arange");

		conn.execute("CREATE TRIGGER `add_on_insert` AFTER INSERT ON `data_point` FOR EACH ROW BEGIN CASE (SELECT (COUNT(*) > 0) FROM `data_stats` WHERE `gid` = NEW.gid) WHEN FALSE THEN BEGIN INSERT INTO `data_stats` (`gid`, `sum`) VALUES (NEW.gid, NEW.value) ; END; ELSE BEGIN UPDATE `data_stats` SET `sum` = `sum` + NEW.value WHERE `gid` = NEW.gid; END; END CASE; END;");
		conn.execute("CREATE TRIGGER `update_on_change` AFTER UPDATE ON `data_point` FOR EACH ROW BEGIN UPDATE `data_stats` SET `sum` = `sum` + (NEW.value - OLD.value) WHERE `gid` = OLD.gid; END;");
		conn.execute("CREATE TRIGGER `subtract_on_delete` AFTER DELETE ON `data_point` FOR EACH ROW BEGIN UPDATE `data_stats` SET `sum` = `sum` - OLD.value WHERE `gid` = OLD.gid; END;");

		conn.execute("INSERT INTO `data_point` (`gid`, `value`) VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0), (1, 5.5)");
		conn.execute("INSERT INTO `data_point` (`gid`, `value`) VALUES (2, 0.1), (2, 0.2), (2, 0.3), (2, 0.4), (2, 0.5)");

		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 1", br(nr, 15.5));
		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 2", br(nr, 1.5));

		conn.execute("UPDATE `data_point` SET `value` = 5.0 WHERE `id` = 5");

		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 1", br(nr, 15.0));
		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 2", br(nr, 1.5));

		conn.execute("DELETE FROM `data_point` WHERE `id` = 3");

		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 1", br(nr, 12.0));
		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 2", br(nr, 1.5));
    }

    // simpler version of D
	@Test
	public void testE() throws Throwable {
		conn.execute("CREATE RANGE arange (int) PERSISTENT GROUP " + checkDDL.getPersistentGroup().getName());

		conn.execute("CREATE TABLE `data_point` (`id` int(11) NOT NULL AUTO_INCREMENT, `gid` int(11) NOT NULL, `value` double NOT NULL, PRIMARY KEY (`id`)) RANGE DISTRIBUTE ON (`id`) USING arange");
		conn.execute("CREATE TABLE `data_stats` (`gid` int(11) NOT NULL, `sum` double NOT NULL) RANGE DISTRIBUTE ON (`gid`) USING arange");

		conn.execute("CREATE TRIGGER `update_on_change` AFTER UPDATE ON `data_point` FOR EACH ROW "
				+"BEGIN UPDATE `data_stats` SET `sum` = `sum` + (NEW.value - OLD.value) WHERE `gid` = OLD.gid; END;");
		
		conn.execute("CREATE TRIGGER `subtract_on_delete` AFTER DELETE ON `data_point` FOR EACH ROW "
				+"BEGIN UPDATE `data_stats` SET `sum` = `sum` - OLD.value WHERE `gid` = OLD.gid; END;");

		conn.execute("INSERT INTO `data_point` (`gid`, `value`) VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0), (1, 5.5)");
		conn.execute("INSERT INTO `data_point` (`gid`, `value`) VALUES (2, 0.1), (2, 0.2), (2, 0.3), (2, 0.4), (2, 0.5)");
		
		conn.execute("INSERT INTO `data_stats` (`gid`, `sum`) VALUES (1,15.5), (2,1.5)");

		conn.execute("UPDATE `data_point` SET `value` = 5.0 WHERE `id` = 5");

		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 1", br(nr, 15.0));

		conn.execute("DELETE FROM `data_point` WHERE `id` = 8");

		conn.assertResults("SELECT `sum` FROM `data_stats` WHERE `gid` = 2", br(nr, 1.2));
	}

	// specifically about handling autoincrements in trigger bodies
	@Test
	public void testF() throws Throwable {
		conn.execute("create range arange (int) persistent group " + checkDDL.getPersistentGroup().getName());
		conn.execute("create table targ (id int auto_increment, diffs varchar(64), primary key (id)) range distribute on (id) using arange");
		conn.execute("create table subj (id int auto_increment, firstname varchar(16), lastname varchar(16), primary key (id)) range distribute on (id) using arange");
	
		// stuff I want to test:
		// before insert: test that autoinc is generated in body
		// after insert: test that target autoinc is propagated to body
		// before update: test that body generates autoincs
		
		conn.execute("create trigger `subj_bef_insert` before insert on `subj` for each row "
				+"begin insert into targ (diffs) values (concat('bef_ins-f:',NEW.firstname,',l:',NEW.lastname,',id:',NEW.ID)); END;");
		
		conn.execute("insert into subj (firstname,lastname) values ('abraham','lincoln'),('james','madison')");
//		System.out.println(conn.printResults("select *, cast(@dve_sitename as char(16)) as site from subj order by id"));
//		System.out.println(conn.printResults("select *, cast(@dve_sitename as char(16)) as site from targ order by id"));
		conn.assertResults("select *,  cast(@dve_sitename as char(16)) as site from subj order by id",
				br(nr,1,"abraham","lincoln","check1",
				   nr,2,"james","madison","check2"));
		conn.assertResults("select *,  cast(@dve_sitename as char(16)) as site from targ order by id",
				br(nr,1,"bef_ins-f:abraham,l:lincoln,id:0","check1",
				   nr,2,"bef_ins-f:james,l:madison,id:0","check2"));

		conn.execute("drop trigger subj_bef_insert");
		conn.execute("truncate subj");
		conn.execute("truncate targ");
		
		conn.execute("create trigger `subj_aft_insert` after insert on `subj` for each row "
				+"begin insert into targ (id,diffs) values (NEW.ID,concat('aft_ins-f:',NEW.firstname,',l:',NEW.lastname,',id:',NEW.ID)); END;");
		conn.execute("insert into subj (firstname,lastname) values ('abraham','lincoln'),('james','madison')");
//		System.out.println(conn.printResults("select *, cast(@dve_sitename as char(16)) as site from subj order by id"));
//		System.out.println(conn.printResults("select *, cast(@dve_sitename as char(16)) as site from targ order by id"));

		conn.assertResults("select *,  cast(@dve_sitename as char(16)) as site from subj order by id",
				br(nr,1,"abraham","lincoln","check1",
				   nr,2,"james","madison","check2"));
		conn.assertResults("select *,  cast(@dve_sitename as char(16)) as site from targ order by id",
				br(nr,1,"aft_ins-f:abraham,l:lincoln,id:1","check1",
				   nr,2,"aft_ins-f:james,l:madison,id:2","check2"));

		conn.execute("create trigger `subj_bef_update` before update on `subj` for each row "
				+"begin insert into targ (diffs) values (concat('bef_update_a-f:',OLD.firstname,'/',NEW.firstname)), (concat('bef_update_b-l:',OLD.lastname,'/',NEW.lastname)); END;");

		conn.execute("update subj set lastname = 'brown' where id = 2");

//		System.out.println(conn.printResults("select *, cast(@dve_sitename as char(16)) as site from subj order by id"));
//		System.out.println(conn.printResults("select *, cast(@dve_sitename as char(16)) as site from targ order by id"));
		
		conn.assertResults("select *,  cast(@dve_sitename as char(16)) as site from subj order by id",
				br(nr,1,"abraham","lincoln","check1",
				   nr,2,"james","brown","check2"));

		conn.assertResults("select *,  cast(@dve_sitename as char(16)) as site from targ order by id",
				br(nr,1,"aft_ins-f:abraham,l:lincoln,id:1","check1",
				   nr,2,"aft_ins-f:james,l:madison,id:2","check2",
				   nr,3,"bef_update_a-f:james/james","check0",
				   nr,4,"bef_update_b-l:madison/brown","check1"));
		
	}
	
	@Test
	public void testPE1672() throws Throwable {
		conn.execute("create table pe1672 ("
				+"attribute_id smallint unsigned not null,"
				+"is_searchable smallint unsigned not null,"
				+"is_visible_in_advanced_search smallint unsigned not null,"
				+"is_filterable smallint unsigned not null,"
				+"is_filterable_in_search smallint unsigned not null,"
				+"used_for_sort_by smallint unsigned not null,"
				+"is_used_for_promo_rules smallint unsigned not null,"
				+"primary key (attribute_id)) broadcast distribute"
				);
		createTriggerTestHelperTables("pe1672");
		conn.execute("create trigger pe1672_after_insert after insert on pe1672 for each row "
				+"BEGIN CASE (NEW.is_searchable = 1) OR (NEW.is_visible_in_advanced_search = 1) OR (NEW.is_filterable > 0) "
				+"  OR (NEW.is_filterable_in_search = 1) OR (NEW.used_for_sort_by = 1) OR (NEW.is_used_for_promo_rules = 1) "
				+"  WHEN TRUE THEN BEGIN UPDATE pe1672_helper as y inner join pe1672_helper_event as ye on y.id = ye.id set "
				+"       y.status = 2 where (event_id = 5); END; ELSE BEGIN END; END CASE; END");
		conn.execute("insert into pe1672 (attribute_id, is_searchable, is_visible_in_advanced_search, is_filterable, is_filterable_in_search, "
				+"used_for_sort_by, is_used_for_promo_rules) values ('134','0','0','0','0','0','0')");
	}
	
	
	@Test
	public void testPE1674() throws Throwable {
		conn.execute("create table pe1674( "
				+"a int unsigned not null auto_increment,"  // config_id
				+"b varchar(8) not null,"					// scope
				+"c int not null,"							// scope_id
				+"d varchar(255) not null,"					// path
				+"e text null,"								// value
				+"primary key (a)) engine=innodb charset=utf8 collate=utf8_general_ci");
		createTriggerTestHelperTables("pe1674");
		conn.execute("create trigger pe1674_after_insert after insert on pe1674 for each row BEGIN "
				+"CASE (NEW.d = 'catalog/price/scope') "
				+"WHEN TRUE THEN "
				+"  BEGIN UPDATE pe1674_helper as h inner join pe1674_helper_event as he on h.id=he.id set h.status = 2 where event_id = 13; END; "
				+"ELSE BEGIN END; END CASE; "
				+"CASE (NEW.d = 'cataloginventory/options/show_out_of_stock') OR (NEW.d = 'cataloginventory/item_options/manage_stock') "
				+"WHEN TRUE THEN "
				+"  BEGIN UPDATE pe1674_helper as h inner join pe1674_helper_event as he on h.id=he.id set h.status = 2 where event_id = 14; END; "
				+"ELSE BEGIN END; END CASE; "
				+"CASE (NEW.d = 'catalog/frontend/flat_catalog_product') AND (NEW.e = 1) "
				+"WHEN TRUE THEN "
				+"  BEGIN UPDATE pe1674_helper as h inner join pe1674_helper_event as he on h.id=he.id set h.status = 2 where event_id = 15; END; "
				+"ELSE BEGIN END; END CASE; "
				+"CASE (NEW.d = 'catalog/frontend/flat_catalog_category') AND (NEW.e = 1) "
				+"WHEN TRUE THEN "
				+"  BEGIN UPDATE pe1674_helper as h inner join pe1674_helper_event as he on h.id=he.id set h.status = 2 where event_id = 16; END; "
				+"ELSE BEGIN END; END CASE; "
				+"END");
		conn.execute("insert into pe1674 (b,c,d,e) values ('default','0','general/region/display_all','1')");
	}
	
	@Test
	public void testBlech() throws Throwable {
		conn.execute("create table blech ("
				+"entity_id int unsigned not null auto_increment, " 
				+"entity_type_id smallint unsigned not null,"		
				+"attribute_set_id smallint unsigned not null default '0',"		
				+"parent_id int unsigned not null,"					
				+"path varchar(255) not null,"					
				+"position int not null,"						
				+"level int not null,"							
				+"children_count int not null,"							
				+"primary key(entity_id)) broadcast distribute");
		conn.execute("create table eurcc ("
				+"version_id bigint unsigned not null auto_increment,"
				+"entity_id int not null,"
				+"primary key (version_id))");
		conn.execute("create table ccpcc ("
				+"version_id bigint unsigned not null auto_increment,"
				+"category_id int not null,"
				+"primary key (version_id))");
		conn.execute("create table ccfc ("
				+"version_id bigint unsigned not null auto_increment,"
				+"entity_id int not null,"
				+"primary key (version_id))");
		conn.execute("create trigger blech_after_insert AFTER INSERT ON blech FOR EACH ROW "
				+"BEGIN "
				+"  INSERT IGNORE INTO eurcc (entity_id) VALUES (NEW.entity_id);"
				+"  INSERT IGNORE INTO ccpcc (category_id) VALUES (NEW.entity_id);"
				+"  INSERT IGNORE INTO ccfc (entity_id) VALUES (NEW.entity_id);"
				+"END");
		conn.execute("insert into blech "
				+"(entity_type_id,parent_id,path,position,level,children_count,entity_id) "
				+"VALUES ('3','0','1', '0', '0', '0', '1')");
	}
	
	private void createTriggerTestHelperTables(String prefix) throws Throwable {
		String helperFormat = 
				"create table %s_helper (id int unsigned not null auto_increment, status smallint not null, primary key (id)) broadcast distribute";
		String helperEventFormat =
				"create table %s_helper_event (some_id int unsigned not null auto_increment, event_id int unsigned not null, "
						+"id int unsigned not null, primary key (some_id)) broadcast distribute";
				
		conn.execute(String.format(helperFormat,prefix));
		conn.execute(String.format(helperEventFormat,prefix));
		
	}
}
