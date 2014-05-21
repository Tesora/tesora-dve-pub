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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DatabaseDDL;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.NativeDatabaseDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PEDatabaseDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class UpdateTest extends ProxySchemaMirrorTest {

	private static final int SITES = 5;

	private static PEDDL buildPEDDL() {
		PEDDL out = new PEDDL();
		StorageGroupDDL sgddl = new StorageGroupDDL("sys",SITES,"sysg");
		out.withStorageGroup(sgddl)
			.withDatabase(new PEDatabaseDDL("sysdb").withStorageGroup(sgddl))
			.withDatabase(new PEDatabaseDDL("tsysdb").withStorageGroup(sgddl));
		return out;
	}

	private static NativeDDL buildNativeDDL() {
		NativeDDL out = new NativeDDL();
		out.withDatabase(new NativeDatabaseDDL("sysdb"))
			.withDatabase(new NativeDatabaseDDL("tsysdb"));
		return out;
	}

	
	private static final ProjectDDL sysDDL = buildPEDDL();

	static final NativeDDL nativeDDL = buildNativeDDL();

	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getSingleDDL() {
		return null;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}
	
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,null,nativeDDL,getSchema());
	}

	static final String[] tabNames = new String[] { "S", "R", "B", "A" };
	static final String[] distVects = new String[] { 
		"static distribute on (`id`)", 
		"range distribute on (`id`) using ", 
		"broadcast distribute",
		"random distribute" };
	private static final String tableBody = "`id` int, `junk` int ";
			
	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				DatabaseDDL db = mr.getDDL().getDatabases().get(0);
				// declare the tables
				ResourceResponse rr = null;
				if (ext) 
					// declare the range
					mr.getConnection().execute("create range open" + db.getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				List<String> actTabs = new ArrayList<String>();
				actTabs.addAll(Arrays.asList(tabNames));
				for(int i = 0; i < actTabs.size(); i++) {
					String tableName = actTabs.get(i);
					String tn = tableName;
					StringBuilder buf = new StringBuilder();
					buf.append("create table `").append(tn).append("` ( ").append(tableBody).append(" ) ");
					if (ext && i < 4) {
						buf.append(distVects[i]);
						if ("R".equals(tabNames[i]))
							buf.append(" open").append(db.getDatabaseName());
					}
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
		});
		return out;
	}

	private List<MirrorTest> getPopulate() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		ArrayList<String> vals = new ArrayList<String>();
		for(int i = 1; i < 10; i++) {
			vals.add("(" + i + "," + i + ")");
		}
		String values = Functional.join(vals, ", ");
		for(int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("delete from " + tabNames[i]));
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + " values " + values));
		}
		return out;
	}
	
	private void populate() throws Throwable {
		List<MirrorTest> inserts = getPopulate();
		TestResource[] tr = new TestResource[] { checkResource, sysResource, nativeResource };
		for(TestResource r : tr) {
			for(MirrorTest mt : inserts) {
				mt.execute(r,null);
			}
		}
	}

	@Override
	protected void onConnect(TestResource tr) throws Throwable {
		tr.getConnection().execute("use " + tr.getDDL().getDatabases().get(0).getDatabaseName());
	}
	

	
	@Test
	public void testBasicUpdate() throws Throwable {
		populate();
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("use sysdb"));
			out.add(new StatementMirrorProc("update " + tabNames[i] + " set junk = 2*junk where junk < 5"));
			out.add(new StatementMirrorProc("use tsysdb"));
			out.add(new StatementMirrorProc("update sysdb." + tabNames[i] + " set id = 10 where junk > 5"));
		}
		runTest(out);
	}
	
	@Test
	public void testPE208_DoubleQuotedLiteral() throws Throwable {
		ConnectionResource conn = sysResource.getConnection();
		
		StringBuffer sql = new StringBuffer();
		sql.append("CREATE TABLE PE208Table(");
		sql.append("place_id int (10) unsigned NOT NULL,");
		sql.append("shows int(10) unsigned DEFAULT '0' NOT NULL,");
		sql.append("ishows int(10) unsigned DEFAULT '0' NOT NULL,");
		sql.append("ushows int(10) unsigned DEFAULT '0' NOT NULL,");
		sql.append("clicks int(10) unsigned DEFAULT '0' NOT NULL,");
		sql.append("iclicks int(10) unsigned DEFAULT '0' NOT NULL,");
		sql.append("uclicks int(10) unsigned DEFAULT '0' NOT NULL,");
		sql.append("sometext varchar(20),");
		sql.append("ts timestamp,");
		sql.append("PRIMARY KEY (place_id,ts)");
		sql.append(");");
		conn.execute(sql.toString());
		
		sql = new StringBuffer();
		sql.append("INSERT INTO PE208Table (");
		sql.append("place_id,shows,ishows,ushows,clicks,iclicks,uclicks,ts) ");
		sql.append("VALUES (1,0,0,0,0,0,0,20000928174434);");
		conn.execute(sql.toString());

		// try to have where with no update first
		sql = new StringBuffer();
		sql.append("UPDATE PE208Table SET ");
		sql.append("shows=shows+1,ishows=ishows+1,ushows=ushows+1,clicks=clicks+1,iclicks=iclicks+1,uclicks=uclicks+1 ");
		sql.append("WHERE place_id=1 AND ts>=\"2020-09-28 00:00:00\"");
		conn.execute(sql.toString());
		conn.assertResults("SELECT place_id, shows, ishows, ushows, clicks, iclicks, uclicks from PE208Table",
				br(nr,Long.valueOf(1),Long.valueOf(0),Long.valueOf(0),Long.valueOf(0),Long.valueOf(0),Long.valueOf(0),Long.valueOf(0)));

		sql = new StringBuffer();
		sql.append("UPDATE PE208Table SET ");
		sql.append("shows=shows+1,ishows=ishows+1,ushows=ushows+1,clicks=clicks+1,iclicks=iclicks+1,uclicks=uclicks+1 ");
		sql.append("WHERE place_id=1 AND ts>=\"2000-09-28 00:00:00\"");
		conn.execute(sql.toString());
		conn.assertResults("SELECT place_id, shows, ishows, ushows, clicks, iclicks, uclicks from PE208Table",
				br(nr,Long.valueOf(1),Long.valueOf(1),Long.valueOf(1),Long.valueOf(1),Long.valueOf(1),Long.valueOf(1),Long.valueOf(1)));
		
		sql = new StringBuffer();
		sql.append("UPDATE PE208Table SET ");
		sql.append("sometext='singlequoted'");
		conn.execute(sql.toString());
		conn.assertResults("SELECT sometext from PE208Table",
				br(nr,"singlequoted"));

		sql = new StringBuffer();
		sql.append("UPDATE PE208Table SET ");
		sql.append("sometext=\"doublequoted\"");
		conn.execute(sql.toString());
		conn.assertResults("SELECT sometext from PE208Table",
				br(nr,"doublequoted"));

		sql = new StringBuffer();
		sql.append("INSERT INTO PE208Table (");
		sql.append("place_id,shows,ishows,ushows,clicks,iclicks,uclicks,sometext,ts) ");
		sql.append("VALUES (2,2,2,2,2,2,2,\"doublequoted2\",20000928174434);");
		conn.execute(sql.toString());
		conn.assertResults("SELECT sometext from PE208Table where place_id=2",
				br(nr,"doublequoted2"));

		conn.assertResults("SELECT 'hello', '\"hello\"', '\"\"hello\"\"', 'hel''lo', '\\\'hello'",
				br(nr,"hello","\"hello\"","\"\"hello\"\"","hel''lo","'hello"));

		conn.assertResults("SELECT \"hello\", \"'hello'\", \"''hello''\", \"hel\"\"lo\", \"\\\"hello\"",
				br(nr,"hello","'hello'","''hello''","hel\"\"lo","\"hello"));
	}	
	
	@Test
	public void testPE564() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String decl = "create table PE564 ( "
						+"`id` int(11) not null auto_increment, " 
						+"`character_id` int(11) not null,"
						+"`quantity` int(11) not null default '1', "
						+"`deleted_from_character_id` int(11) default null,"
						+"`equipped_quantity` int(11) not null default '0',"
						+"`hand_item_id` int(11) default null,"
						+"`body_item_id` int(11) default null,"
						+"`companion_item_id` int(11) default null,"
						+"primary key (`id`),"
						+"key `character_id` (`character_id`)"
						+") engine=innodb"; 
				if (!nativeDDL.equals(mr.getDDL())) {
					DatabaseDDL db = mr.getDDL().getDatabases().get(0);
					String rangeName = "open" + db.getDatabaseName();
					return mr.getConnection().execute(decl + " range distribute on (`character_id`) using " + rangeName);
				}
				return mr.getConnection().execute(decl);
			}			
		});
		tests.add(new StatementMirrorProc("update `PE564` "
				+"set `quantity` = 0, `equipped_quantity` = 0, `deleted_from_character_id` = 5458, "
				+"`character_id` = 0, `hand_item_id` = NULL, `body_item_id` = NULL, `companion_item_id` = NULL "
				+"where `PE564`.`id` = 23797"));
		runTest(tests);
	}
	
	@Test
	public void testPE1366() throws Throwable {
		final ConnectionResource conn = sysResource.getConnection();

		conn.execute("create table pe1366 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int, UNIQUE (`desc`)) range distribute on (`id`) using opensysdb");
		conn.execute("insert into pe1366 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("update pe1366 set `desc` = 'Tim' where `id` = 3");
			}
		}.assertException(PEException.class);
	}
	
	@Test
	public void testUpdateIgnore() throws Throwable {
		final ConnectionResource conn = sysResource.getConnection();

		/* Update on distribution vector. */
		 conn.execute("create table Range1 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int) range distribute on (`id`) using opensysdb");
		 conn.execute("insert into Range1 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");
		 conn.assertResults("select COUNT(*) from Range1 where `desc` = 'Petr'",
		 br(nr, 0l));
		 conn.execute("update ignore Range1 set `id` = 8, `desc` = 'Petr' where `flags` = 0");
		 conn.assertResults("select COUNT(*) from Range1 where `desc` = 'Petr'",
		 br(nr, 1l));

		/* Non-DV, unique column update. */
		conn.execute("create table Range2 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int, UNIQUE (`desc`)) range distribute on (`id`) using opensysdb");
		conn.execute("insert into Range2 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");
		conn.assertResults("select COUNT(*) from Range2 where `desc` = 'Petr'", br(nr, 0l));
		conn.execute("update ignore Range2 set `desc` = 'Petr' where `flags` = 0");
		conn.assertResults("select COUNT(*) from Range2 where `desc` = 'Petr'", br(nr, 1l));

		/* Non-DV, multi-column unique key update. */
		conn.execute("create table Range3 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int, UNIQUE (`desc`, `flags`)) range distribute on (`id`) using opensysdb");
		conn.execute("insert into Range3 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");
		conn.assertResults("select COUNT(*) from Range3 where `desc` = 'Petr'", br(nr, 0l));
		conn.execute("update ignore Range3 set `desc` = 'Petr' where `id` in (6, 7)");
		conn.assertResults("select COUNT(*) from Range3 where `desc` = 'Petr'", br(nr, 2l));

		/*
		 * It is a known limitation of the Random distribution model that it can
		 * lead to duplicate unique entries on INSERT/UPDATE.
		 */
//		conn.execute("create table Random1 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int, UNIQUE (`desc`)) random distribute");
//		conn.execute("insert into Random1 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");
//		conn.assertResults("select COUNT(*) from Random1 where `desc` = 'Petr'", br(nr, 0l));
//		conn.execute("update ignore Random1 set `desc` = 'Petr' where `flags` = 0");
//		conn.assertResults("select COUNT(*) from Random1 where `desc` = 'Petr'", br(nr, 1l));

		/* Broadcast unique column update. */
		conn.execute("create table Broadcast1 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int, UNIQUE (`desc`)) broadcast distribute");
		conn.execute("insert into Broadcast1 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");
		conn.assertResults("select COUNT(*) from Broadcast1 where `desc` = 'Petr'", br(nr, 0l));
		conn.execute("update ignore Broadcast1 set `desc` = 'Petr' where `flags` = 0");
		conn.assertResults("select COUNT(*) from Broadcast1 where `desc` = 'Petr'", br(nr, 1l));

		/* Broadcast multi-column unique key update. */
		conn.execute("create table Broadcast2 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int, UNIQUE (`desc`, `flags`)) broadcast distribute");
		conn.execute("insert into Broadcast2 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");
		conn.assertResults("select COUNT(*) from Broadcast2 where `desc` = 'Petr'", br(nr, 0l));
		conn.execute("update ignore Broadcast2 set `desc` = 'Petr' where `id` in (6, 7)");
		conn.assertResults("select COUNT(*) from Broadcast2 where `desc` = 'Petr'", br(nr, 2l));
	}

	@Test
	public void testPE771() throws Throwable {
		final ConnectionResource conn = sysResource.getConnection();

		conn.execute("create table pe771 (id int unsigned not null auto_increment, code tinyint unsigned not null, name char(20) not null, primary key (id), key (code), unique (name)) engine=MyISAM RANGE DISTRIBUTE ON (`id`) USING `opensysdb`");
		conn.execute("insert into pe771 (`code`, `name`) values (1, 'Tim'), (1, 'Monty'), (2, 'David'), (2, 'Erik'), (3, 'Sasha'), (3, 'Jeremy'), (4, 'Matt')");
		conn.assertResults("select COUNT(*) from pe771 where `name` = 'Sinisa'", br(nr, 0l));
		conn.execute("update ignore pe771 set id = 8, name = 'Sinisa' where `id` < 3");
		conn.assertResults("select COUNT(*) from pe771 where `name` = 'Sinisa'", br(nr, 1l));
	}
}
