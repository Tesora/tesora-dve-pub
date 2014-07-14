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
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.primitives.Bytes;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class InsertTest extends SchemaMirrorTest {
	private static final int SITES = 5;

	private static final ProjectDDL sysDDL = new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	static final NativeDDL nativeDDL = new NativeDDL("cdb");
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,null,nativeDDL,getSchema());
	}

	static final String[] tabNames = new String[] { "B", "S", "A", "R" }; 
	static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using "
	};
	private static final String tabBody = 
		" `id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)";
	
	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				// declare the tables
				ResourceResponse rr = null;
				if (ext) 
					// declare the range
					mr.getConnection().execute("create range open" + mr.getDDL().getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				List<String> actTabs = new ArrayList<String>();
				actTabs.addAll(Arrays.asList(tabNames));
				// actTabs.add("T");
				for(int i = 0; i < actTabs.size(); i++) {
					String tn = actTabs.get(i);
					StringBuilder buf = new StringBuilder();
					buf.append("create table `").append(tn).append("` ( ").append(tabBody).append(" ) ");
					if (ext && i < 4) {
						buf.append(distVects[i]);
						if ("R".equals(tabNames[i]))
							buf.append(" open").append(mr.getDDL().getDatabaseName());
					}
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
		});
		return out;
	}	
	
	private void forAll(String template, List<MirrorTest> acc, String[] tables) {
		for(int i = 0; i < tables.length; i++) {
			final String actual = template.replace("#", tables[i]);
			acc.add(new StatementMirrorProc(actual));
		}
	}

	
	@Test
	public void testSingleInsert() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		forAll("delete from #", tests, tabNames);
		forAll("insert into # (`junk`) values ('trash')",tests,tabNames);
		forAll("delete from #", tests, tabNames);
		runTest(tests);
	}
	
	@Test
	public void testMultiInsert() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		forAll("delete from #",tests,tabNames);
		forAll("insert into # (`junk`) values ('a'), ('b'), ('c'), ('d')", tests, tabNames);
		forAll("delete from #", tests, tabNames);
		runTest(tests);
	}

	@Test
	public void testExponentialNotationInsert() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("create table mathi (`id` int, `data` float)"));
		tests.add(new StatementMirrorProc("insert into mathi values (1,8e-05)"));
		tests.add(new StatementMirrorProc("insert into mathi values (2,-1e+05)"));
		tests.add(new StatementMirrorFun("select * from mathi order by id"));
		runTest(tests);
	}
	
	@Test
	public void testTruncate() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("create table trunctest (`id` int auto_increment, `junk` varchar(32), primary key (`id`)) /*#dve static distribute on (`id`) */"));
		tests.add(new StatementMirrorProc("insert into trunctest (`junk`) values ('a'), ('b'), ('c'), ('d'), ('e'), ('f'), ('g')"));
		tests.add(new StatementMirrorFun("select * from trunctest order by id"));
		tests.add(new StatementMirrorProc("truncate trunctest"));
		tests.add(new StatementMirrorProc("insert into trunctest (`junk`) values ('a'), ('b'), ('c'), ('d'), ('e'), ('f'), ('g')"));
		tests.add(new StatementMirrorFun("select * from trunctest order by id"));
		runTest(tests);
	}
	
	// not sure if this is a real issue yet
	@Ignore
	@Test
	public void testCaseSensitivity() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("create table new_order (no_w_id integer not null, no_d_id integer not null, no_o_id integer not null)"));
		tests.add(new StatementMirrorProc("INSERT INTO new_order (`new_order`.`no_o_id`, `new_order`.`no_d_id`, `new_order`.`no_w_id`) values (3001, 5, 1)"));
		tests.add(new StatementMirrorProc("INSERT INTO NEW_ORDER (`new_order`.`no_o_id`, `new_order`.`no_d_id`, `new_order`.`no_w_id`) values (3001, 5, 1)"));
		runTest(tests);
	}
	
	@Ignore
	@Test
	public void testBigInsert() throws Throwable {
		
		StringBuffer buf = new StringBuffer();
		buf.append("insert into # values ");
		for(int i = 1; i < 1001; i++) {
			if (i > 1)
				buf.append(", ");
			buf.append("(").append(i).append(", '").append(i).append("')");
		}
		ListOfPairs<TestResource,TestResource> config = new ListOfPairs<TestResource,TestResource>();
		config.add(sysResource, null);
		
		// first time - turn off the plan cache and delete any old data
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("alter dve set plan_cache_limit = 0"));
		forAll("delete from #",tests,tabNames);
		runTest(tests, config, false);
		
		int runs = 5;
		
		ListOfPairs<String,Long> times = new ListOfPairs<String,Long>();
		
		// there's quite a bit of variability in this, so we're going to run it 5 times and only consider the last time
		for(int i = 0; i <= runs; i++) {
			boolean report = i == runs;
			for(String tn : tabNames) {
				tests.clear();
				String[] just = new String[] { tn };
				forAll(buf.toString(), tests, just);
				long startAt = System.nanoTime();
				runTest(tests,config,false);
				long delta = System.nanoTime() - startAt;
				long microseconds = delta/1000;
				// System.out.println("Table " + tn + " took " + microseconds);
				if (report)
					times.add(tn, microseconds);
			}
			tests.clear();
			forAll("delete from #",tests,tabNames);
			runTest(tests,config,false);
		}
		
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		String minTab = null;
		String maxTab = null;
		for(Pair<String,Long> p : times) {
			if (p.getSecond().longValue() > max) {
				max = p.getSecond().longValue();
				maxTab = p.getFirst();
			}
			if (p.getSecond().longValue() < min) {
				min = p.getSecond().longValue();
				minTab = p.getFirst();
			}
		}
		// really the variability is usually around 2x - but make it 4 so it's pretty reliable
		// also, that's better than what it used to be, which was around 15
		int factor = 4;
		if (max > (factor * min)) {
			fail("Slowest table " + maxTab + " took " + max + " which is more than " + factor +"x slower than fastest table " + minTab + " which took " + min);
		}
	}
	
	@Test
	public void testInsertBinaryLiteral() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		String decl = "CREATE TABLE `cm` (" 
				  + "`cid` varchar(255) NOT NULL DEFAULT '',"
				  +"`data` longblob,"
				  +"`exp` int(11) NOT NULL DEFAULT '0',"
				  +"`crt` int(11) NOT NULL DEFAULT '0',"
				  +"`srz` smallint(6) NOT NULL DEFAULT '0',"
				  +"PRIMARY KEY (`cid`),"
				  +"KEY `exp` (`exp`)"
				+") ENGINE=InnoDB DEFAULT CHARSET=utf8";
		tests.add(new StatementMirrorProc(decl));		
	
		tests.add(new StatementMirrorProc("INSERT INTO `cm` VALUES ('links:main-menu:page:node:en:1:1','a:4:{s:9:\"min_depth\";i:1;s:9:\"max_depth\";i:1;s:8:\"expanded\";a:1:{i:0;i:0;}s:12:\"active_trail\";a:1:{i:0;i:0;}}',0,1368701074,1)," 
				+"('links:main-menu:tree-data:en:9ec01ec58bf82a695e4acd636af283e0585fe8cd8a6e54eb140188a3e284ab1c','a:2:{s:4:\"tree\";a:1:{i:218;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:9:\"main-menu\";s:4:\"mlid\";s:3:\"218\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:7:\"<front>\";s:11:\"router_path\";s:0:\"\";s:10:\"link_title\";s:4:\"Home\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:4:\"menu\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"1\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:3:\"218\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";N;s:16:\"to_arg_functions\";N;s:15:\"access_callback\";N;s:16:\"access_arguments\";N;s:13:\"page_callback\";N;s:14:\"page_arguments\";N;s:17:\"delivery_callback\";N;s:10:\"tab_parent\";N;s:8:\"tab_root\";N;s:5:\"title\";N;s:14:\"title_callback\";N;s:15:\"title_arguments\";N;s:14:\"theme_callback\";N;s:15:\"theme_arguments\";N;s:4:\"type\";N;s:11:\"description\";N;s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}}s:10:\"node_links\";a:0:{}}',0,1368701074,1)" 
				+",('links:navigation:page:node:en:1:0','a:4:{s:9:\"min_depth\";i:1;s:9:\"max_depth\";N;s:8:\"expanded\";a:1:{i:0;i:0;}s:12:\"active_trail\";a:1:{i:0;i:0;}}',0,1368701074,1),('links:navigation:tree-data:en:9bd1605e2280833450478f9083b7f8714c2fa28f1012455e2744e5af1a13eec5','a:2:{s:4:\"tree\";a:8:{i:3;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:1:\"3\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:9:\"comment/%\";s:11:\"router_path\";s:9:\"comment/%\";s:10:\"link_title\";s:17:\"Comment permalink\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"1\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:1:\"3\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:12:\"a:1:{i:1;N;}\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:11:\"user_access\";s:16:\"access_arguments\";s:33:\"a:1:{i:0;s:15:\"access comments\";}\";s:13:\"page_callback\";s:17:\"comment_permalink\";s:14:\"page_arguments\";s:14:\"a:1:{i:0;i:1;}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:9:\"comment/%\";s:5:\"title\";s:17:\"Comment permalink\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:4;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:1:\"4\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:11:\"filter/tips\";s:11:\"router_path\";s:11:\"filter/tips\";s:10:\"link_title\";s:12:\"Compose tips\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"1\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:1:\"4\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:0:\"\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:1:\"1\";s:16:\"access_arguments\";s:6:\"a:0:{}\";s:13:\"page_callback\";s:16:\"filter_tips_long\";s:14:\"page_arguments\";s:6:\"a:0:{}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:11:\"filter/tips\";s:5:\"title\";s:12:\"Compose tips\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:2:\"20\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:5;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:1:\"5\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:6:\"node/%\";s:11:\"router_path\";s:6:\"node/%\";s:10:\"link_title\";s:0:\"\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:1:\"5\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:26:\"a:1:{i:1;s:9:\"node_load\";}\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:11:\"node_access\";s:16:\"access_arguments\";s:29:\"a:2:{i:0;s:4:\"view\";i:1;i:1;}\";s:13:\"page_callback\";s:14:\"node_page_view\";s:14:\"page_arguments\";s:14:\"a:1:{i:0;i:1;}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:6:\"node/%\";s:5:\"title\";s:0:\"\";s:14:\"title_callback\";s:15:\"node_page_title\";s:15:\"title_arguments\";s:14:\"a:1:{i:0;i:1;}\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:6;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:1:\"6\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:8:\"node/add\";s:11:\"router_path\";s:8:\"node/add\";s:10:\"link_title\";s:11:\"Add content\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"1\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:1:\"6\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:0:\"\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:16:\"_node_add_access\";s:16:\"access_arguments\";s:6:\"a:0:{}\";s:13:\"page_callback\";s:13:\"node_add_page\";s:14:\"page_arguments\";s:6:\"a:0:{}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:8:\"node/add\";s:5:\"title\";s:11:\"Add content\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:17;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:2:\"17\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:6:\"user/%\";s:11:\"router_path\";s:6:\"user/%\";s:10:\"link_title\";s:10:\"My account\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"1\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:2:\"17\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:26:\"a:1:{i:1;s:9:\"user_load\";}\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:16:\"user_view_access\";s:16:\"access_arguments\";s:14:\"a:1:{i:0;i:1;}\";s:13:\"page_callback\";s:14:\"user_view_page\";s:14:\"page_arguments\";s:14:\"a:1:{i:0;i:1;}\";s:17:\"delivery_callback\";s:0:\"\";"
				+"s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:6:\"user/%\";s:5:\"title\";s:10:\"My account\";s:14:\"title_callback\";s:15:\"user_page_title\";s:15:\"title_arguments\";s:14:\"a:1:{i:0;i:1;}\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:23;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:2:\"23\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:15:\"comment/reply/%\";s:11:\"router_path\";s:15:\"comment/reply/%\";s:10:\"link_title\";s:15:\"Add new comment\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:2:\"23\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:26:\"a:1:{i:2;s:9:\"node_load\";}\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:11:\"node_access\";s:16:\"access_arguments\";s:29:\"a:2:{i:0;s:4:\"view\";i:1;i:2;}\";s:13:\"page_callback\";s:13:\"comment_reply\";s:14:\"page_arguments\";s:14:\"a:1:{i:0;i:2;}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:15:\"comment/reply/%\";s:5:\"title\";s:15:\"Add new comment\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:27;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:2:\"27\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:15:\"taxonomy/term/%\";s:11:\"router_path\";s:15:\"taxonomy/term/%\";s:10:\"link_title\";s:13:\"Taxonomy term\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:2:\"27\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:36:\"a:1:{i:2;s:18:\"taxonomy_term_load\";}\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:11:\"user_access\";s:16:\"access_arguments\";s:32:\"a:1:{i:0;s:14:\"access content\";}\";s:13:\"page_callback\";s:18:\"taxonomy_term_page\";s:14:\"page_arguments\";s:14:\"a:1:{i:0;i:2;}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:15:\"taxonomy/term/%\";s:5:\"title\";s:13:\"Taxonomy term\";s:14:\"title_callback\";s:19:\"taxonomy_term_title\";s:15:\"title_arguments\";s:14:\"a:1:{i:0;i:2;}\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:187;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:10:\"navigation\";s:4:\"mlid\";s:3:\"187\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:6:\"search\";s:11:\"router_path\";s:6:\"search\";s:10:\"link_title\";s:6:\"Search\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"1\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:1:\"0\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:3:\"187\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:0:\"\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:16:\"search_is_active\";s:16:\"access_arguments\";s:6:\"a:0:{}\";s:13:\"page_callback\";s:11:\"search_view\";s:14:\"page_arguments\";s:6:\"a:0:{}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:6:\"search\";s:5:\"title\";s:6:\"Search\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:2:\"20\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}}s:10:\"node_links\";a:0:{}}',0,1368701074,1),('links:shortcut-set-1:page:node:en:1:0','a:4:{s:9:\"min_depth\";i:1;s:9:\"max_depth\";N;s:8:\"expanded\";a:1:{i:0;i:0;}s:12:\"active_trail\";a:1:{i:0;i:0;}}',0,1368701074,1),('links:shortcut-set-1:tree-data:en:9bd1605e2280833450478f9083b7f8714c2fa28f1012455e2744e5af1a13eec5','a:2:{s:4:\"tree\";a:2:{i:216;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:14:\"shortcut-set-1\";s:4:\"mlid\";s:3:\"216\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:8:\"node/add\";s:11:\"router_path\";s:8:\"node/add\";s:10:\"link_title\";s:11:\"Add content\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:4:\"menu\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:3:\"-20\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:3:\"216\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:0:\"\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:16:\"_node_add_access\";s:16:\"access_arguments\";s:6:\"a:0:{}\";s:13:\"page_callback\";s:13:\"node_add_page\";s:14:\"page_arguments\";s:6:\"a:0:{}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:8:\"node/add\";s:5:\"title\";s:11:\"Add content\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:217;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:14:\"shortcut-set-1\";s:4:\"mlid\";s:3:\"217\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:13:\"admin/content\";s:11:\"router_path\";s:13:\"admin/content\";s:10:\"link_title\";s:12:\"Find content\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:4:\"menu\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:3:\"-19\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:3:\"217\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:0:\"\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:11:\"user_access\";s:16:\"access_arguments\";s:41:\"a:1:{i:0;s:23:\"access content overview\";}\";s:13:\"page_callback\";s:15:\"drupal_get_form\";s:14:\"page_arguments\";s:36:\"a:1:{i:0;s:18:\"node_admin_content\";}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:13:\"admin/content\";s:5:\"title\";s:7:\"Content\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:32:\"Administer content and comments.\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}}s:10:\"node_links\";a:0:{}}',0,1368701074,1),('links:user-menu:page:node:en:1:1','a:4:{s:9:\"min_depth\";i:1;s:9:\"max_depth\";i:1;s:8:\"expanded\";a:1:{i:0;i:0;}s:12:\"active_trail\";a:1:{i:0;i:0;}}',0,1368701074,1)" 
				+",('links:user-menu:tree-data:en:9ec01ec58bf82a695e4acd636af283e0585fe8cd8a6e54eb140188a3e284ab1c','a:2:{s:4:\"tree\";a:2:{i:2;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:9:\"user-menu\";s:4:\"mlid\";s:1:\"2\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:4:\"user\";s:11:\"router_path\";s:4:\"user\";s:10:\"link_title\";s:1"
				+"2:\"User account\";s:7:\"options\";s:22:\"a:1:{s:5:\"alter\";b:1;}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:3:\"-10\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:1:\"2\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:0:\"\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:1:\"1\";s:16:\"access_arguments\";s:6:\"a:0:{}\";s:13:\"page_callback\";s:9:\"user_page\";s:14:\"page_arguments\";s:6:\"a:0:{}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:4:\"user\";s:5:\"title\";s:12:\"User account\";s:14:\"title_callback\";s:15:\"user_menu_title\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}i:15;a:2:{s:4:\"link\";a:42:{s:9:\"menu_name\";s:9:\"user-menu\";s:4:\"mlid\";s:2:\"15\";s:4:\"plid\";s:1:\"0\";s:9:\"link_path\";s:11:\"user/logout\";s:11:\"router_path\";s:11:\"user/logout\";s:10:\"link_title\";s:7:\"Log out\";s:7:\"options\";s:6:\"a:0:{}\";s:6:\"module\";s:6:\"system\";s:6:\"hidden\";s:1:\"0\";s:8:\"external\";s:1:\"0\";s:12:\"has_children\";s:1:\"0\";s:8:\"expanded\";s:1:\"0\";s:6:\"weight\";s:2:\"10\";s:5:\"depth\";s:1:\"1\";s:10:\"customized\";s:1:\"0\";s:2:\"p1\";s:2:\"15\";s:2:\"p2\";s:1:\"0\";s:2:\"p3\";s:1:\"0\";s:2:\"p4\";s:1:\"0\";s:2:\"p5\";s:1:\"0\";s:2:\"p6\";s:1:\"0\";s:2:\"p7\";s:1:\"0\";s:2:\"p8\";s:1:\"0\";s:2:\"p9\";s:1:\"0\";s:7:\"updated\";s:1:\"0\";s:14:\"load_functions\";s:0:\"\";s:16:\"to_arg_functions\";s:0:\"\";s:15:\"access_callback\";s:17:\"user_is_logged_in\";s:16:\"access_arguments\";s:6:\"a:0:{}\";s:13:\"page_callback\";s:11:\"user_logout\";s:14:\"page_arguments\";s:6:\"a:0:{}\";s:17:\"delivery_callback\";s:0:\"\";s:10:\"tab_parent\";s:0:\"\";s:8:\"tab_root\";s:11:\"user/logout\";s:5:\"title\";s:7:\"Log out\";s:14:\"title_callback\";s:1:\"t\";s:15:\"title_arguments\";s:0:\"\";s:14:\"theme_callback\";s:0:\"\";s:15:\"theme_arguments\";s:6:\"a:0:{}\";s:4:\"type\";s:1:\"6\";s:11:\"description\";s:0:\"\";s:15:\"in_active_trail\";b:0;}s:5:\"below\";a:0:{}}}s:10:\"node_links\";a:0:{}}',0,1368701074,1)" 
				+",('menu_custom','a:5:{s:5:\"devel\";a:3:{s:9:\"menu_name\";s:5:\"devel\";s:5:\"title\";s:11:\"Development\";s:11:\"description\";s:16:\"Development link\";}s:9:\"main-menu\";a:3:{s:9:\"menu_name\";s:9:\"main-menu\";s:5:\"title\";s:9:\"Main menu\";s:11:\"description\";s:115:\"The <em>Main</em> menu is used on many sites to show the major sections of the site, often in a top navigation bar.\";}s:10:\"management\";a:3:{s:9:\"menu_name\";s:10:\"management\";s:5:\"title\";s:10:\"Management\";s:11:\"description\";s:69:\"The <em>Management</em> menu contains links for administrative tasks.\";}s:10:\"navigation\";a:3:{s:9:\"menu_name\";s:10:\"navigation\";s:5:\"title\";s:10:\"Navigation\";s:11:\"description\";s:150:\"The <em>Navigation</em> menu contains links intended for site visitors. Links are added to the <em>Navigation</em> menu automatically by some modules.\";}s:9:\"user-menu\";a:3:{s:9:\"menu_name\";s:9:\"user-menu\";s:5:\"title\";s:9:\"User menu\";s:11:\"description\";s:99:\"The <em>User</em> menu contains links related to the user\\'s account, as well as the \\'Log out\\' link.\";}}',0,1368701074,1)"
				));
		tests.add(new StatementMirrorFun("select * from cm order by cid"));
		
		runTest(tests);
	}
	
	@Ignore
	@Test
	public void testMultiCreateDrop() throws Throwable {
		ListOfPairs<TestResource,TestResource> config = new ListOfPairs<TestResource,TestResource>();
		config.add(sysResource, null);
		for(int i = 0; i < 1000000; i++) {
			List<MirrorTest> tests = new ArrayList<MirrorTest>();
			tests.add(new StatementMirrorProc("create table suspiria (`id` int auto_increment, `iteration` varchar(32), primary key (`id`)) range distribute on (`id`) using opensysdb"));
			tests.add(new StatementMirrorProc("insert into suspiria (`iteration`) values ('a-" + i + "'),('b-" + i + "'),('c-" + i + "')"));
			tests.add(new StatementMirrorFun("select * from suspiria order by id"));
			tests.add(new StatementMirrorProc("drop table suspiria"));
			runTest(tests,config,false);
		}
	}
	
	public void testPE584() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		String decl = "create table pe584 ("
				+"`id` int not null, "
				+"`inum` int not null, "
				+"`sinum` smallint not null, "
				+"`bnum` bit not null, "
				+"`cstr` char(3) not null, "
				+"`vstr` varchar(3) not null, "
				+"`d1` date not null,"
				+"`t1` time not null,"
				+"primary key(`id`)) engine=innodb /*#dve broadcast distribute */";
		tests.add(new StatementMirrorProc(decl));		
/*
		tests.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				System.out.println("resource " + mr.getDDL().getDatabaseName() + " has sql_mode of " + mr.getConnection().printResults("select @@sql_mode"));
				return null;
			}
			
		});
		*/
//		tests.add(new StatementMirrorProc("/* first time */insert into pe584 (id) values (1)"));
		String[] modes = new String[] { "strict_trans_tables", "" };
		for(String m : modes) {
			final String fm = m;
			tests.add(new StatementMirrorProc("delete from pe584"));
			tests.add(new StatementMirrorProc("set sql_mode='" + m + "'"));
			tests.add(new MirrorProc() {

				@Override
				public ResourceResponse execute(TestResource mr) throws Throwable {
					String stmt = "/* mode = " + fm + "*/insert into pe584 (id) values (1)";
					try {
						mr.getConnection().execute(stmt);
						if ("strict_trans_tables".equals(fm))
							fail("under strict trans tables shouldn't be able to insert without values");
					} catch (Exception e) {
						if (!"strict_trans_tables".equals(fm))
							throw e;
					}
					return null;
				}
				
			});
		}
		tests.add(new StatementMirrorProc("drop table pe584"));
		runTest(tests);
	}

	@Test
	public void testPE579() throws Throwable {
		String tbl = "`PE579`";
		String decl = null;
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		decl = "create table " + tbl + " ( "
		+"`id` int(11) not null auto_increment, " 
		+"`data` int(11) not null,"
		+"primary key (`id`)"
		+") engine=innodb /*#dve range distribute on (`id`) using opensysdb */";
		tests.add(new StatementMirrorProc(decl));
		tests.add(new StatementMirrorApply("insert delayed into " + tbl + " (data) values (1)"));
		tests.add(new StatementMirrorFun("select * from " + tbl + ""));
		tests.add(new StatementMirrorApply("insert delayed ignore into " + tbl + " (id, data) values (1,2)"));
		tests.add(new StatementMirrorFun("select * from " + tbl + ""));
		tests.add(new StatementMirrorProc("insert ignore into " + tbl + " (data) values (1)"));
		tests.add(new StatementMirrorFun("select * from " + tbl + ""));
		tests.add(new StatementMirrorProc("insert ignore into " + tbl + " (id, data) values (1,2)"));
		tests.add(new StatementMirrorFun("select * from " + tbl + ""));
		tests.add(new StatementMirrorProc("drop table " + tbl + ""));


		tbl = "`PE579_isam`";
		decl = "create table " + tbl + " ( "
				+"`id` int(11) not null auto_increment, " 
				+"`data` int(11) not null,"
				+"primary key (`id`)"
				+") engine=myisam /*#dve range distribute on (`id`) using opensysdb */";
		tests.add(new StatementMirrorProc(decl));
		tests.add(new StatementMirrorApply("insert delayed into " + tbl + " (data) values (1)"));
		tests.add(new StatementMirrorFun(true, "/* first */select * from " + tbl + ""));
		tests.add(new StatementMirrorApply("insert delayed ignore into " + tbl + " (id, data) values (1,2)"));
		tests.add(new StatementMirrorFun(true, "/* second */select * from " + tbl + ""));
		tests.add(new StatementMirrorProc("insert ignore into " + tbl + " (data) values (1)"));
		tests.add(new StatementMirrorFun(true, "/* third */select * from " + tbl + ""));
		tests.add(new StatementMirrorProc("insert ignore into " + tbl + " (id, data) values (1,2)"));
		tests.add(new StatementMirrorFun(true, "/* fourth */select * from " + tbl + ""));
		tests.add(new StatementMirrorProc("drop table " + tbl + ""));
		runTest(tests);
	}
	
	@Test
	public void testPE588And620() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		String decl = "create table pe588 (`id` int auto_increment, `sid` int, `type` varchar(8), primary key (`id`), unique key (`sid`)) engine=innodb /*#dve broadcast distribute */";
		tests.add(new StatementMirrorProc(decl));
		tests.add(new StatementMirrorProc("insert into pe588 set sid=2, type='BS'"));
		tests.add(new StatementMirrorFun("select * from pe588"));
		tests.add(new StatementMirrorProc("drop table pe588"));
		runTest(tests);
	}
	
	@Test
	public void testPE647() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("set names 'utf8'"));
		String decl = "CREATE TABLE `pe647` (`keyname` varchar(100) NOT NULL ,`title` text NOT NULL ,`body` longtext NOT NULL ,`imgurl` varchar(255) NOT NULL ,`linkurl` varchar(255) NOT NULL ,`footer` varchar(255) NOT NULL ,`countrycode` varchar(2) NOT NULL ,`gender` varchar(1) NOT NULL ,`add_date` datetime NOT NULL) ENGINE=InnoDB  CHARSET=utf8";	
		tests.add(new StatementMirrorProc(decl));
		tests.add(new StatementMirrorProc("INSERT INTO pe647 VALUES (\"EVENTHEROAUF\", \"Céline Luggage \", \"The craze over the Celine Luggage has been at a fever pitch for many seasons and it definitely shows no sings of diminishing. The aesthetic features of the bag are so contagious that this particular 'It' bag managed to translate to almost everybody. From neutral to bold colours and classic leathers to luxurious accents there is no wonder why everybody needs one. \",\"http://www.reebonz.com/sites/all/files/celine_luggage-1.jpg\",\"http://www.reebonz.com/redirect/node/1996034\",\"Event Starts: 2013-02-23 09:00 (GMT +8) - 2013-02-26 06:00 (GMT+8)\",\"AU\",\"F\",\"2013-02-23\")"));
		tests.add(new StatementMirrorFun("select * from pe647"));
		tests.add(new StatementMirrorProc("drop table pe647"));
		runTest(tests);
	}
	
	@Test
	public void testPE670() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		String decl = "CREATE TABLE `watchdog` ( " +
				  "`wid` int(11) NOT NULL AUTO_INCREMENT, " +
				  "`uid` int(11) NOT NULL DEFAULT '0', " +
				  "`type` varchar(16) NOT NULL DEFAULT '', " +
				  "`message` longtext NOT NULL, " +
				  "`variables` longtext NOT NULL, " +
				  "`severity` tinyint(3) unsigned NOT NULL DEFAULT '0', " +
				  "`link` varchar(255) NOT NULL DEFAULT '', " +
				  "`location` text NOT NULL, " +
				  "`referer` varchar(128) NOT NULL DEFAULT '', " +
				  "`hostname` varchar(128) NOT NULL DEFAULT '', " +
				  "`timestamp` int(11) NOT NULL DEFAULT '0', " +
				  "PRIMARY KEY (`wid`), " +
				  "KEY `type` (`type`), " +
				  "KEY `uid` (`uid`), " +
				  "FULLTEXT KEY `variables` (`variables`) " +
				") ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=938785 /*#dve  BROADCAST DISTRIBUTE */ ";		
		tests.add(new StatementMirrorProc(decl));
		tests.add(new StatementMirrorProc(
				"INSERT DELAYED INTO watchdog " +
						"(uid, type, message, variables, severity, link, location, referer, hostname, timestamp) " +
						"VALUES " +
						"(0, 'uc_paypal_mobile', 'paypal_ipn_mobile:<pre>@arr</pre>', 'a:1:{s:4:\"@arr\";s:1318:\"Array\n(\n    [mc_gross] => -16200\n    [invoice] => 430774\n    [protection_eligibility] => Ineligible\n    [payer_id] => MXHVZDA6V4MC2\n    [address_street] => ¼ª°²¡¡ÖÐÕýÂ·Ò»¶Î22¡¡\n    [payment_date] => 23:01:58 Mar 07, 2013 PST\n    [payment_status] => Refunded\n    [charset] => gb2312\n    [address_zip] => 973\n    [first_name] => ¡¡Þ±\n    [mc_fee] => -399\n    [address_country_code] => TW\n    [address_name] => ¡¡Þ± ¡¡\n    [notify_version] => 3.7\n    [reason_code] => refund\n    [custom] => \n    [address_country] => Taiwan\n    [address_city] => »¨¡¡¡¡\n    [verify_sign] => ArgX1JoEl.LNTJ5nkTmgzkfLaDiVAtcJAg86pax1puv10Wg9NusnjXdu\n    [payer_email] => vilma.huang@gmail.com\n    [parent_txn_id] => 4B287026L6847863V\n    [txn_id] => 9C394351VR491402V\n    [payment_type] => instant\n    [last_name] => ¡¡\n    [address_state] => \n    [receiver_email] => paypal@reebonz.com\n    [payment_fee] => \n    [receiver_id] => S24M54R2JKW5G\n    [item_name] => 1x Loewe Billetero Continental Wallet (1966544-18281F117440)\n    [mc_currency] => TWD\n    [item_number] => \n    [residence_country] => TW\n    [receipt_id] => 1744-0473-7178-9561\n    [handling_amount] => 0\n    [transaction_subject] => 1x Loewe Billetero Continental Wallet (1966544-18281F117440)\n    [payment_gross] => \n    [shipping] => 0\n    [ipn_track_id] => 4a36c9b097c44\n)\n\";}', 7, '', 'http://www.reebonz.com/uc_paypal_mobile/ipn/430774', '', '173.0.81.1', 1362726123)"));		
		runTest(tests);
	}
	
	@Test
	public void testPE693() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("create table pe693(`id` int auto_increment, `fid` int, primary key (`id`)) /*#dve broadcast distribute */"));
		tests.add(new StatementMirrorProc("insert into pe693 (`fid`) values (2)"));
		tests.add(new StatementMirrorProc("insert into pe693 (`id`,`fid`) values (0,4),(null,6)"));
		tests.add(new StatementMirrorFun("select * from pe693 order by id"));
		tests.add(new StatementMirrorProc("set @old_sql_mode = @@sql_mode"));
		tests.add(new StatementMirrorProc("set sql_mode='NO_AUTO_VALUE_ON_ZERO'"));
		tests.add(new StatementMirrorProc("insert into pe693 (`id`,`fid`) values (0,12)"));
		tests.add(new StatementMirrorFun("select * from pe693 order by id"));
		tests.add(new StatementMirrorProc("set sql_mode = @old_sql_mode"));
		tests.add(new StatementMirrorProc("insert into pe693 (`id`,`fid`) values (0,22),(null,24)"));
		tests.add(new StatementMirrorFun("select * from pe693 order by id"));
		runTest(tests);
	}

	@Test
	public void testPE748() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		String decl = "CREATE TABLE `pe748` ( " +
				  "`id` int(11) NOT NULL, " +
				  "PRIMARY KEY (`id`) " +
				") /*#dve  BROADCAST DISTRIBUTE */ ";		
		tests.add(new StatementMirrorProc(decl));
		tests.add(new StatementMirrorFun("select * from pe748"));
		
		tests.add(new StatementMirrorProc("BEGIN"));
		tests.add(new StatementMirrorProc("INSERT INTO pe748 VALUES (1)"));		
		tests.add(new StatementMirrorProc("ROLLBACK"));
		tests.add(new StatementMirrorFun("select * from pe748"));

		tests.add(new StatementMirrorProc("BEGIN"));
		tests.add(new StatementMirrorProc("INSERT INTO pe748 VALUES (1)"));		
		tests.add(new StatementMirrorProc("COMMIT"));
		tests.add(new StatementMirrorFun("select * from pe748"));

		tests.add(new StatementMirrorProc("BEGIN WORK"));
		tests.add(new StatementMirrorProc("INSERT INTO pe748 VALUES (2)"));		
		tests.add(new StatementMirrorProc("ROLLBACK WORK"));
		tests.add(new StatementMirrorFun("select * from pe748"));

		tests.add(new StatementMirrorProc("BEGIN WORK"));
		tests.add(new StatementMirrorProc("INSERT INTO pe748 VALUES (2)"));		
		tests.add(new StatementMirrorProc("COMMIT WORK"));
		tests.add(new StatementMirrorFun("select * from pe748"));

		runTest(tests);
	}

	/**
	 * Handle '0' prefixed numbers correctly (not as octals)
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testPE820() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		String createSql = "CREATE TABLE `pe820` ( " +
				  "`id` int(11) NOT NULL, " +
				  "PRIMARY KEY (`id`) " +
				") /*#dve  BROADCAST DISTRIBUTE */ ";		
		tests.add(new StatementMirrorProc(createSql));
		tests.add(new StatementMirrorFun("select * from pe820"));
		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (1)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (-1)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (+2)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (-00)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (08)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (+0188)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (-0299)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (0x100)"));		
		//tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (+0x101)"));		
		//tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (-0x102)"));		
		tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (0xfe)"));		
		//tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (-0xfd)"));		
		//tests.add(new StatementMirrorProc("INSERT INTO pe820 VALUES (+0xff)"));		
		tests.add(new StatementMirrorFun("select * from pe820"));

		runTest(tests);
	}

	@Test
	public void testPE1036_distroTester() throws Throwable {
		try (ProxyConnectionResource rootConnection= new ProxyConnectionResource()) {
			rootConnection.execute("use " + sysDDL.getDatabaseName());

			StringBuilder buf = new StringBuilder();

			buf.append("CREATE RANGE `r_nstdb` ( integer, smallint, tinyint, datetime, binary(47) ) PERSISTENT GROUP " + sysDDL.getPersistentGroup().getName());
			rootConnection.execute(buf.toString());

			buf = new StringBuilder();
			buf.append("CREATE TABLE `t0` ( dvix int, rix int, c0 int, c1 tinyint, c2 integer, c3 binary (11), " +
					"c4 binary (49) , c5 smallint , c6 integer , c7 int , c8 char (28) collate latin1_general_cs, " +
					"c9 tinyint, c10 tinyint, c11 varchar (25) collate latin1_general_cs, c12 datetime, c13 smallint, " +
					"c14 smallint, c15 binary(47) ) RANGE DISTRIBUTE ON ( c2, c14, c1, c12, c15 ) USING `r_nstdb`");
			rootConnection.execute(buf.toString());

			buf = new StringBuilder();
			buf.append("INSERT INTO `t0` VALUES ");
			buf.append("  ( 28, 0, 2033452789, 39, 1639496434, '42678819995', '5387601639528504378310911920837567427500762713160', 4600, 948059779, 306495563, 'rAp8uctcBX,esHJJ1w=zDWs(/329', -120, 93, 'K1AO^]y:A45V;rI7jNlwQg)Ma', '2009-07-08 03:38:32', 30647, 23579, 'athad^DFHw45ysh$W%DFhah$#Ydhf'),");
			buf.append("  ( 24, 1, 53698802, 88, 1676423847, '21774101414', '9026008622383463856799564887203305027082230799284', -26548, 529048145, 2126243193, 'DwQ[Cu<g5anPK7jS6QSGS1eS4bue', -114, 96, 'dOLY9XyupqnLL=] l^jfdI-TM', '2018-11-17 18:08:03', 9731, -5170, 'by%$s4bysbr$S^B45bayZGebYAYEN'),");
			buf.append("  ( 1, 2, 1001517541, -126, 2130144761, '21957118741', '6524899441842940220162031349182744345282478862004', 23988, 2063510532, 2081194380, 'M44adJtBNSaH:A.,EgAmX&wc5m^B', 116, 40, 'Xp#V0t9skX-oYsw4uEjou7gqa', '2010-06-13 00:37:22', 20033, -15698, 'cjngrsth$%n4USRUSNRuSXFhtbs'),");
			buf.append("  ( 21, 3, 1781486194, -104, 1728257688, '56819708342', '3603233650288232621379199119473943488754074009381', -10086, 1908385802, 1509473499, 'BGz2(6x(aCbs3UVWe-<p chirkn2', -45, 11, 'QU&;t?U<@O4CvWjcU^^GklFrK', '2015-10-19 09:16:46', 21896, -24696, 'fu4$U4uBSRTU^unsrbysRB'),");
			buf.append("  ( 1, 4, 1001517541, -96, 2130144761, '36988992458', '9959834414042752311147256348798883788431855222474', -15559, 2009183998, 218698240, 'M44adJtBNSaH:A.,EgAmX&wc5m^B', -77, -62, 'XLTnFdWZTDGbrLRgszHCrng9q', '2012-09-29 06:19:53', 9096, -15698, '47%^&Bs4ubsRUBS%uns4YGSRB$^')");
			rootConnection.execute(buf.toString());
		}
	}
	
	/**
	 * This test is supposed to reproduce behavior exhibited by the native MySQL
	 * client. When inserting a duplicate primary key into a BC table. The
	 * client correctly throws an "Duplicate entry for key 'PRIMARY'" error, but
	 * loses connection on the subsequent statement.
	 */
	@Test
	public void testPE1193() throws Throwable {
		try (final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource()) {
			connection.execute("USE " + sysDDL.getDatabaseName());
			connection.execute("CREATE TABLE IF NOT EXISTS pe1193 (id INT NOT NULL, PRIMARY KEY (id)) BROADCAST DISTRIBUTE");
			connection.execute("INSERT INTO pe1193 VALUES (1)");

			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					connection.execute("INSERT INTO pe1193 VALUES (1)");
				}
			}.assertException(Exception.class, "Duplicate entry '1' for key 'PRIMARY'");

			connection.execute("INSERT INTO pe1193 VALUES (2)");
		}
	}

	@Test
	public void testPE1124_largeColumnCount() throws Throwable {
		try (ProxyConnectionResource rootConnection = new ProxyConnectionResource()) {
			rootConnection.execute("use " + sysDDL.getDatabaseName());

			StringBuilder buf = new StringBuilder();
			buf.append("CREATE TABLE t1 (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT, ");
			buf.append("c11 INT, c12 INT, c13 INT, c14 INT, c15 INT, c16 INT, c17 INT, c18 INT, c19 INT, c20 INT, ");
			buf.append("c21 INT, c22 INT, c23 INT, c24 INT, c25 INT, c26 INT, c27 INT, c28 INT, c29 INT, c30 INT, ");
			buf.append("c31 INT, c32 INT, c33 INT, c34 INT, c35 INT, c36 INT, c37 INT, c38 INT, c39 INT, c40 INT, ");
			buf.append("c41 INT, c42 INT, c43 INT, c44 INT, c45 INT, c46 INT, c47 INT, c48 INT, c49 INT, c50 INT, ");
			buf.append("c51 INT, c52 INT, c53 INT, c54 INT, c55 INT, c56 INT, c57 INT, c58 INT, c59 INT, c60 INT, ");
			buf.append("c61 INT, c62 INT, c63 INT, c64 INT, c65 INT, c66 INT, c67 INT, c68 INT, c69 INT, c70 INT, ");
			buf.append("c71 INT, c72 INT, c73 INT, c74 INT, c75 INT, c76 INT, c77 INT, c78 INT, c79 INT, c80 INT, ");
			buf.append("c81 INT, c82 INT, c83 INT, c84 INT, c85 INT, c86 INT, c87 INT, c88 INT, c89 INT, c90 INT, ");
			buf.append("c91 INT, c92 INT, c93 INT, c94 INT, c95 INT, c96 INT, c97 INT, c98 INT, c99 INT, c100 INT, ");
			buf.append("c101 INT, c102 INT, c103 INT, c104 INT, c105 INT, c106 INT, c107 INT, c108 INT, c109 INT, c110 INT, ");
			buf.append("c111 INT, c112 INT, c113 INT, c114 INT, c115 INT, c116 INT, c117 INT, c118 INT, c119 INT, c120 INT, ");
			buf.append("c121 INT, c122 INT, c123 INT, c124 INT, c125 INT, c126 INT, c127 INT, c128 INT, c129 INT, c130 INT, ");
			buf.append("c131 INT, c132 INT, c133 INT, c134 INT, c135 INT, c136 INT, c137 INT, c138 INT, c139 INT, c140 INT, ");
			buf.append("c141 INT, c142 INT, c143 INT, c144 INT, c145 INT, c146 INT, c147 INT, c148 INT, c149 INT, c150 INT, ");
			buf.append("c151 INT, c152 INT, c153 INT, c154 INT, c155 INT, c156 INT, c157 INT, c158 INT, c159 INT, c160 INT, ");
			buf.append("c161 INT, c162 INT, c163 INT, c164 INT, c165 INT, c166 INT, c167 INT, c168 INT, c169 INT, c170 INT, ");
			buf.append("c171 INT, c172 INT, c173 INT, c174 INT, c175 INT, c176 INT, c177 INT, c178 INT, c179 INT, c180 INT, ");
			buf.append("c181 INT, c182 INT, c183 INT, c184 INT, c185 INT, c186 INT, c187 INT, c188 INT, c189 INT, c190 INT, ");
			buf.append("c191 INT, c192 INT, c193 INT, c194 INT, c195 INT, c196 INT, c197 INT, c198 INT, c199 INT, c200 INT, ");
			buf.append("c201 INT, c202 INT, c203 INT, c204 INT, c205 INT, c206 INT, c207 INT, c208 INT, c209 INT, c210 INT, ");
			buf.append("c211 INT, c212 INT, c213 INT, c214 INT, c215 INT, c216 INT, c217 INT, c218 INT, c219 INT, c220 INT, ");
			buf.append("c221 INT, c222 INT, c223 INT, c224 INT, c225 INT, c226 INT, c227 INT, c228 INT, c229 INT, c230 INT, ");
			buf.append("c231 INT, c232 INT, c233 INT, c234 INT, c235 INT, c236 INT, c237 INT, c238 INT, c239 INT, c240 INT, ");
			buf.append("c241 INT, c242 INT, c243 INT, c244 INT, c245 INT, c246 INT, c247 INT, c248 INT, c249 INT, c250 INT, ");
			buf.append("c251 INT, c252 INT, c253 INT, c254 INT, c255 INT, c256 INT, c257 INT, c258 INT)");

			rootConnection.execute(buf.toString());

			buf = new StringBuilder();
			buf.append("INSERT INTO `t1` (c1) VALUES (1)");
			rootConnection.execute(buf.toString());

			try {
				buf = new StringBuilder();
				buf.append("SELECT * FROM t1");
				rootConnection.execute(buf.toString());
			} catch (Throwable e) {
				// comment this out to test hang for PE-1124 (after commenting out fix in MysqlExecuteCommand.processAwaitFieldCount)
				fail("Should not throw exception: " + PEStringUtils.toString(e));
			}

			buf = new StringBuilder();
			buf.append("SELECT c1 FROM t1");
			rootConnection.execute(buf.toString());
		}
	}
	
	@Test
	public void testPE1149() throws Throwable {
		final ProxyConnectionResource pcr = new ProxyConnectionResource();

		try {
			final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			tests.add(new StatementMirrorProc(
					"CREATE TABLE `bug_repro_1149` (`user_session_id` varchar(32) NOT NULL,`detail` blob, PRIMARY KEY (`user_session_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"));
			final byte[] failingInput = { (byte) 255 };	// any value above 127 will fail
			final List<Byte> binarySql = new ArrayList<Byte>();
			binarySql.addAll(Bytes.asList("INSERT INTO `bug_repro_1149` VALUES ('somekey','".getBytes()));
			binarySql.addAll(Bytes.asList(failingInput));
			binarySql.addAll(Bytes.asList("')".getBytes()));
			binaryTestHelper(pcr, binarySql, tests);
			// The line below is commented out because we can't figure out why the values are different when the data in the database is the same
//			tests.add(new StatementMirrorFun("SELECT * from `bug_repro_1149`"));
	
			runTest(tests);
		} finally {
			pcr.disconnect();
		}
	}
	
	private void binaryTestHelper(final ProxyConnectionResource pcr, List<Byte> binarySql, ArrayList<MirrorTest> tests) throws Throwable {
		final byte[] backingBinaryArray = ArrayUtils.toPrimitive(binarySql.toArray(new Byte[] {}));
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr.getDDL().isNative()) {
					return mr.getConnection().execute(new String(backingBinaryArray, CharsetUtil.ISO_8859_1));
				} else {
					pcr.execute("use " + mr.getDDL().getDatabaseName());
					return pcr.execute(null, backingBinaryArray);
				}
			}
		});
	}

	@Test
	public void testPE1222_PE1239() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		String createSql = "CREATE TABLE `ic_x_user_community` (`USER_ID` varchar(32) NOT NULL DEFAULT '',`COMMUNITY_ID` varchar(32) NOT NULL DEFAULT '',`HUB_PAGE` varchar(100) NOT NULL DEFAULT '/apps/shell/frames.html',`IS_ENABLED` tinyint(4) NOT NULL DEFAULT '1',PRIMARY KEY (`USER_ID`,`COMMUNITY_ID`),KEY `IC_X_USER_COMMUNITY_IDX_01` (`COMMUNITY_ID`,`USER_ID`)) ENGINE=InnoDB DEFAULT CHARSET=utf8; ";		
		tests.add(new StatementMirrorProc(createSql));
		tests.add(new StatementMirrorProc("SET NAMES 'utf8' COLLATE 'utf8_general_ci'"));
		tests.add(new StatementMirrorProc("INSERT IGNORE INTO `ic_x_user_community` VALUES ( NAME_CONST('user_id',_latin1'1df417a8-3063-11e3-b92e-000c2969'), 'SRI', '/sri/index.html', NAME_CONST('is_enabled',1)); "));		
		tests.add(new StatementMirrorFun("select * from `ic_x_user_community` order by `USER_ID`"));
		tests.add(new StatementMirrorProc("INSERT IGNORE INTO `ic_x_user_community` VALUES ( NAME_CONST('user_id',_latin1'2df417a8-3063-11e3-b92e-000c2969' COLLATE latin1_swedish_ci), 'SRI', '/sri/index.html', 1); "));		
		tests.add(new StatementMirrorFun("select * from `ic_x_user_community` order by `USER_ID`"));
		tests.add(new StatementMirrorProc("INSERT IGNORE INTO `ic_x_user_community` VALUES ( NAME_CONST('user_id',_latin1'3df417a8-3063-11e3-b92e-000c2969' COLLATE 'latin1_swedish_ci'), 'SRI', '/sri/index.html', 1); "));		
		tests.add(new StatementMirrorFun("select * from `ic_x_user_community` order by `USER_ID`"));

		runTest(tests);
	}

	@Test
	@Ignore
	public void testPE1366() throws Throwable {
		final ConnectionResource conn = sysResource.getConnection();

		conn.execute("create table Range2 (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int, UNIQUE (`desc`)) range distribute on (`id`) using opensysdb");
		conn.execute("insert into Range2 (`id`, `desc`, `flags`) values (1, 'Tim', 0), (2, 'Monty', 0), (3, 'David', 1), (4, 'Erik', 0), (5, 'Sasha', 1), (6, 'Jeremy', 1), (7, 'Matt', 0)");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("insert into Range2 (`id`, `desc`, `flags`) values (8, 'Tim', 0)");
			}
		}.assertException(PEException.class);
	}

	@Test
	public void testPE1397() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("create table pe1397_users (user_id int, f1 int, f2 int) /*#dve broadcast distribute */"));
		tests.add(new StatementMirrorProc("create table pe1397_followers (user_id int, follower int) /*#dve broadcast distribute */"));
		tests.add(new StatementMirrorProc("insert into pe1397_users (user_id) values (1),(2),(3),(4),(5),(6),(7)"));
		tests.add(new StatementMirrorProc("insert into pe1397_followers values (1,2),(2,3),(2,4),(3,4),(3,5),(3,6),(4,5),(4,6),(4,7),(4,1),(4,2)"));
		tests.add(new StatementMirrorProc("create table pe1397_f1 (user_id int, ct int)"));
		tests.add(new StatementMirrorProc("create table pe1397_f2 (user_id int, ct int)"));
		tests.add(new StatementMirrorProc("create table pe1397_f3 (follower int, ct int)"));
		tests.add(new StatementMirrorProc("/* a */insert into pe1397_f1 select user_id, count(user_id) from pe1397_followers group by user_id"));
//		tests.add(new StatementMirrorFun(true,"select follower, count(follower) from pe1397_followers group by follower"));
		tests.add(new StatementMirrorProc("/* b */insert into pe1397_f2 select follower, count(follower) from pe1397_followers group by follower"));
		tests.add(new StatementMirrorProc("/* c */insert into pe1397_f3 select follower, count(follower) from pe1397_followers group by follower"));
		tests.add(new StatementMirrorFun("select count(*) from pe1397_f1"));
		tests.add(new StatementMirrorFun("select count(*) from pe1397_f2"));
		tests.add(new StatementMirrorFun("select count(*) from pe1397_f3"));
		runTest(tests);
	}

	@Test
	public void testPE348() throws Throwable {
		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();

		tests.add(new StatementMirrorProc(
				"CREATE TABLE pe348 (p Point NOT NULL, ls LineString NOT NULL, pg Polygon NOT NULL, mp MultiPoint NOT NULL, mls MultiLineString NOT NULL, mpg MultiPolygon NOT NULL, g Geometry NOT NULL, gc GeometryCollection NOT NULL)"));
		tests.add(new StatementMirrorProc(
				"INSERT INTO pe348 VALUES ("
						+ "Point(30, 10), "
						+ "LineString(Point(30, 10), Point(10, 30), Point(40, 40)),"
						+ "Polygon(LineString(Point(35, 10), Point(45, 45), Point(15, 40), Point(10, 20), Point(35, 10)), LineString(Point(20, 30), Point(35, 35), Point(30, 20), Point(20, 30))), "
						+ "MultiPoint(Point(10, 40), Point(40, 30), Point(20, 20), Point(30, 10)), "
						+ "MultiLineString(LineString(Point(10, 10), Point(20, 20), Point(10, 40)), LineString(Point(40, 40), Point(30, 30), Point(40, 20), Point(30, 10))), "
						+ "MultiPolygon(Polygon(LineString(Point(40, 40), Point(20, 45), Point(45, 30), Point(40, 40))), Polygon(LineString(Point(20, 35), Point(10, 30), Point(10, 10), Point(30, 5), Point(45, 20), Point(20, 35)), LineString(Point(30, 20), Point(20, 15), Point(20, 25), Point(30, 20)))), "
						+ "Point(0, 0), "
						+ "GeometryCollection("
						+ "Point(30, 10), "
						+ "LineString(Point(30, 10), Point(10, 30), Point(40, 40)),"
						+ "Polygon(LineString(Point(35, 10), Point(45, 45), Point(15, 40), Point(10, 20), Point(35, 10)), LineString(Point(20, 30), Point(35, 35), Point(30, 20), Point(20, 30))), "
						+ "MultiPoint(Point(10, 40), Point(40, 30), Point(20, 20), Point(30, 10)), "
						+ "MultiLineString(LineString(Point(10, 10), Point(20, 20), Point(10, 40)), LineString(Point(40, 40), Point(30, 30), Point(40, 20), Point(30, 10))), "
						+ "MultiPolygon(Polygon(LineString(Point(40, 40), Point(20, 45), Point(45, 30), Point(40, 40))), Polygon(LineString(Point(20, 35), Point(10, 30), Point(10, 10), Point(30, 5), Point(45, 20), Point(20, 35)), LineString(Point(30, 20), Point(20, 15), Point(20, 25), Point(30, 20))))"
						+ ")"
						+ ")"));
		tests.add(new StatementMirrorProc(
				"INSERT INTO pe348 VALUES ("
						+ "GeomFromText('POINT (30 10)'), "
						+ "GeomFromText('LINESTRING (30 10, 10 30, 40 40)'), "
						+ "GeomFromText('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))'), "
						+ "GeomFromText('MULTIPOINT (10 40, 40 30, 20 20, 30 10)'), "
						+ "GeomFromText('MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))'), "
						+ "GeomFromText('MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))'), "
						+ "GeomFromText('POINT (0 0)'), "
						+ "GeomFromText('GEOMETRYCOLLECTION ("
						+ "POINT (30 10), "
						+ "LINESTRING (30 10, 10 30, 40 40), "
						+ "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30)), "
						+ "MULTIPOINT (10 40, 40 30, 20 20, 30 10), "
						+ "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10)), "
						+ "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))))"
						+ "')"
						+ ")"));
		tests.add(new StatementMirrorFun("SELECT * FROM pe348"));
		tests.add(new StatementMirrorFun("SELECT AsText(p), AsText(ls), AsText(pg), AsText(mp), AsText(mls), AsText(mpg), AsText(g), AsText(gc) FROM pe348"));

		runTest(tests);
	}
	
	@Test
	public void testPE1547() throws Throwable {
		ResourceResponse.BLOB_COLUMN.useFormatedOutput(false);
		try {
			final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			tests.add(new StatementMirrorProc(
					"CREATE TABLE `pe1547` ("
							+ "`cid` varchar(255) NOT NULL DEFAULT '' COMMENT 'Primary Key: Unique cache ID.',"
							+ "`data` longblob COMMENT 'A collection of data to cache.',"
							+ "`expire` int(11) NOT NULL DEFAULT '0' COMMENT 'A Unix timestamp indicating when the cache entry should expire, or 0 for never.',"
							+ "`created` int(11) NOT NULL DEFAULT '0' COMMENT 'A Unix timestamp indicating when the cache entry was created.',"
							+ "`serialized` smallint(6) NOT NULL DEFAULT '0' COMMENT 'A flag to indicate whether content is serialized (1) or not (0).',"
							+ "PRIMARY KEY (`cid`),"
							+ "KEY `expire` (`expire`)"
							+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Cache table used to store node entity records.';"));

			tests.add(new StatementMirrorProc(
					"INSERT INTO `pe1547` VALUES ('3','jícama jícama',0,1400117996,1)"));
			tests.add(new StatementMirrorProc(
					"INSERT INTO `pe1547` VALUES ('4','jícama jícama',0,1400117996,1),('5','xxx',0,1400117996,1)"));

			tests.add(new StatementMirrorFun("SELECT * FROM pe1547 ORDER BY cid"));

			runTest(tests);
		} finally {
			ResourceResponse.BLOB_COLUMN.useFormatedOutput(true);
		}
	}
	
	@Test
	public void testPE1569() throws Throwable {
		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc(
				"CREATE TABLE `test` ("
						+ "`cid` varchar(255),"
						+ "`data` varchar(255) "
						+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8"));

		tests.add(new StatementMirrorProc("INSERT INTO test VALUES ('0', '000'); INSERT INTO test VALUES ('1', '111'); INSERT INTO test VALUES ('2', '222');"));
		tests.add(new StatementMirrorFun("SELECT * FROM test ORDER BY cid;"));

		tests.add(new StatementMirrorProc("SET AUTOCOMMIT = 0"));
		tests.add(new StatementMirrorProc("INSERT INTO test VALUES ('4', '444'); COMMIT;"));
		tests.add(new StatementMirrorProc("ROLLBACK"));
		tests.add(new StatementMirrorFun("SELECT * FROM test ORDER BY cid"));

		runTest(tests);
	}

}
