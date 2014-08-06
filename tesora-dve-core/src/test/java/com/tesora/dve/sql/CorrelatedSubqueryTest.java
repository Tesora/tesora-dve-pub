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
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.worker.agent.Agent;

public class CorrelatedSubqueryTest extends SchemaMirrorTest {

	
	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	private static final ProjectDDL checkDDL =
			null;
//		new PEDDL("checkdb",
//				new StorageGroupDDL("check",1,"checkg"),
//				"schema");
	static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getSingleDDL() {
		return checkDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL, checkDDL, nativeDDL, getPopulate());
	}

	static final String[] tabNames = new String[] { "B", "S", "A", "R" }; 
	static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`e`)",
		"random distribute",
		"range distribute on (`e`) using "
	};

	// needs a different population than the agg test
	// for correlated subquery we need to have a bunch of repeated values
	// so we are going to do powers of 2
	private static final String tabBody =
			" `id` int auto_increment, `e` int, `d` int, `c` int, `b` int, `a` int, primary key (id) ";
	
	private static List<MirrorTest> getPopulate() {
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
		ArrayList<String> tuples = new ArrayList<String>();
		int strata = 1;  
		String format = "(%d,%d,%d,%d,%d)";
		for(int a = 1; a <= strata; a++) {
			for(int b = 1; b <= (2*strata); b++) {
				for(int c = 1; c <= (4*strata); c++) {
					for(int d = 1; d <= (8*strata); d++) {
						for(int e = 1; e <= (16*strata); e++) {
							tuples.add(String.format(format,e,d,c,b,a));
						}
					}
				}
			}
		}
		ArrayList<String> reordered = new ArrayList<String>();
		while(!tuples.isEmpty()) {
			int ith = Agent.getRandom(tuples.size());
			reordered.add(tuples.remove(ith));
		}
		String rest = "(`e`,`d`,`c`,`b`,`a`) values " + Functional.join(reordered, ", ");
		for(int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + rest));
		}		
		return out;
	}

	@Test
	public void testSetup() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(int i = 0; i < tabNames.length; i++) {
			tests.add(new StatementMirrorFun("select count(*) from " + tabNames[i]));
		}
		runTest(tests);
	}
	
	private List<MirrorTest> generate(String format, String[] ltabs, String[] rtabs) {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int l = 0; l < ltabs.length; l++) {
			for(int r = 0; r < rtabs.length; r++) {
				if (l == r) continue;
				// ignore the metadata for now
				out.add(new StatementMirrorFun(true,true,String.format(format,ltabs[l],rtabs[r])));
			}
		}
		return out;
	}
	
	/* population hint:
	 * mysql> select max(a), min(a), max(b), min(b), max(c), min(c), max(d), min(d), max(e), min(e) from A;
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
| max(a) | min(a) | max(b) | min(b) | max(c) | min(c) | max(d) | min(d) | max(e) | min(e) |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|      1 |      1 |      2 |      1 |      4 |      1 |      8 |      1 |     16 |      1 |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+

	 */
	
	@Test
	public void testA() throws Throwable {
		List<MirrorTest> tests =
				generate("select count(l.a) from %s l where l.d = (select max(r.c) from %s r where l.e = r.e)",
						tabNames, tabNames);
		runTest(tests);
	}
	
	@Test
	public void testB() throws Throwable {
		List<MirrorTest> tests =
				generate("select r.id, (select avg(l.c) from %s l where l.b = r.c) from %s r where r.c in (2,3) and r.e in (7,8) order by r.id",
						tabNames, tabNames);
		runTest(tests);
	}
	
	// more to come, but not right now

	@Test
	public void testPE1231() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("/*#dve create range keyword_range (int) persistent group sysg */"));
		tests.add(new StatementMirrorProc("CREATE TABLE `categories` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`name` varchar(255) NOT NULL,`created` datetime NOT NULL,`modified` datetime NOT NULL,`account_id` int(11) unsigned NOT NULL,PRIMARY KEY (`id`),UNIQUE KEY `uk_cat_name_account` (`name`,`account_id`),KEY `idx_categories_1` (`name`),KEY `fk_categories_account` (`account_id`)) ENGINE=InnoDB AUTO_INCREMENT=109862 DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */"));
		tests.add(new StatementMirrorProc("CREATE TABLE `keywords` (`id` int(10) unsigned NOT NULL,`domain_id` int(10) unsigned NOT NULL,`keyword` varchar(1) NOT NULL,`created` datetime NOT NULL,`modified` datetime NOT NULL,`prime` bit(1) NOT NULL,`raw_keyword` varchar(1) DEFAULT NULL,PRIMARY KEY (`id`),UNIQUE KEY `domain_id_2` (`domain_id`, `keyword`),KEY `domain_id` (`domain_id`, `prime`),KEY `keyword` (`keyword`),KEY `raw_keyword` (`raw_keyword`)) ENGINE=InnoDB DEFAULT CHARSET=utf8  /*#dve BROADCAST DISTRIBUTE */"));
		tests.add(new StatementMirrorProc("CREATE TABLE `keyword_rank` (`keyword_id` int(10) unsigned NOT NULL,`time_period_id` int(10) unsigned NOT NULL,`keyword_url_relationship_type_id` tinyint(3) unsigned NOT NULL,`rank_type_id` smallint(5) unsigned NOT NULL,`url_domain_id` int(10) unsigned NOT NULL,`keyword_domain_id` int(10) unsigned NOT NULL,`rank` tinyint(3) unsigned NOT NULL,`url_id` int(10) unsigned DEFAULT NULL,PRIMARY KEY (`keyword_id`, `keyword_url_relationship_type_id`, `rank_type_id`, `time_period_id`, `url_domain_id`),KEY `time_period_id` (`time_period_id`),KEY `keyword_url_relationship_type_id` (`keyword_url_relationship_type_id`),KEY `url_domain_id` (`url_domain_id`),KEY `rank_type_id` (`rank_type_id`),KEY `keyword_domain_id` (`keyword_domain_id`, `keyword_url_relationship_type_id`, `time_period_id`, `url_domain_id`, `rank_type_id`),KEY `keyword_domain_id_2` (`keyword_domain_id`, `time_period_id`, `rank_type_id`, `rank`, `url_domain_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8  /*#dve RANGE DISTRIBUTE ON (`keyword_id`) USING `keyword_range` */"));
		tests.add(new StatementMirrorProc("CREATE TABLE `keyword_categories` (`category_id` int(11) unsigned NOT NULL,`keyword_id` int(11) unsigned NOT NULL,PRIMARY KEY (`category_id`,`keyword_id`),KEY `fk_kc_keyword` (`keyword_id`),CONSTRAINT `fk_kc_category` FOREIGN KEY (`category_id`) REFERENCES `categories` (`id`),CONSTRAINT `fk_kc_keyword` FOREIGN KEY (`keyword_id`) REFERENCES `keywords` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */"));
		tests.add(new StatementMirrorFun("select keyword0_.id as id1_24_, keyword0_.created as created2_24_, keyword0_.domain_id as domain_i7_24_, keyword0_.keyword as keyword3_24_, keyword0_.modified as modified4_24_, keyword0_.prime as prime5_24_, keyword0_.raw_keyword as raw_keyw6_24_ from keywords keyword0_ where keyword0_.domain_id=292 and (exists (select 1 from keyword_rank keywordran1_ where keywordran1_.keyword_id=keyword0_.id and keywordran1_.time_period_id=213)) and (keyword0_.id in (select keyword4_.id from categories keywordcat2_ inner join keyword_categories keywords3_ on keywordcat2_.id=keywords3_.category_id inner join keywords keyword4_ on keywords3_.keyword_id=keyword4_.id where keywordcat2_.id=49673)) limit 2147483647"));

		runTest(tests);
	}
	
	@Ignore
	@Test
	public void testPE1558() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();

		tests.add(new StatementMirrorProc("DROP TABLE IF EXISTS taxonomy_term_data"));
		tests.add(new StatementMirrorProc("DROP TABLE IF EXISTS node"));
		tests.add(new StatementMirrorProc("DROP TABLE IF EXISTS taxonomy_index"));
		tests.add(new StatementMirrorProc("DROP TABLE IF EXISTS taxonomy_vocabulary"));

		tests.add(new StatementMirrorProc("CREATE TABLE `taxonomy_term_data` ("
				+ "`tid` int(10) unsigned NOT NULL COMMENT 'Primary Key: Unique term ID.',"
				+ "`vid` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'The taxonomy_vocabulary.vid of the vocabulary to which the term is assigned.',"
				+ "`name` varchar(255) NOT NULL DEFAULT '' COMMENT 'The term name.',"
				+ "`description` longtext COMMENT 'A description of the term.',"
				+ "`format` varchar(255) DEFAULT NULL COMMENT 'The filter_format.format of the description.',"
				+ "`weight` int(11) NOT NULL DEFAULT '0' COMMENT 'The weight of this term in relation to other terms.',"
				+ "PRIMARY KEY (`tid`),"
				+ "KEY `taxonomy_tree` (`vid`,`weight`,`name`),"
				+ "KEY `vid_name` (`vid`,`name`),"
				+ "KEY `name` (`name`)"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Stores term information.' /*#dve BROADCAST DISTRIBUTE */"));
		tests.add(new StatementMirrorProc("CREATE TABLE `node` ("
				+ "`nid` int(10) unsigned NOT NULL COMMENT 'The primary identifier for a node.',"
				+ "`vid` int(10) unsigned DEFAULT NULL COMMENT 'The current node_revision.vid version identifier.',"
				+ "`type` varchar(32) NOT NULL DEFAULT '' COMMENT 'The node_type.type of this node.',"
				+ "`language` varchar(12) NOT NULL DEFAULT '' COMMENT 'The languages.language of this node.',"
				+ "`title` varchar(255) NOT NULL DEFAULT '' COMMENT 'The title of this node, always treated as non-markup plain text.',"
				+ "`uid` int(11) NOT NULL DEFAULT '0' COMMENT 'The users.uid that owns this node; initially, this is the user that created it.',"
				+ "`status` int(11) NOT NULL DEFAULT '1' COMMENT 'Boolean indicating whether the node is published (visible to non-administrators).',"
				+ "`created` int(11) NOT NULL DEFAULT '0' COMMENT 'The Unix timestamp when the node was created.',"
				+ "`changed` int(11) NOT NULL DEFAULT '0' COMMENT 'The Unix timestamp when the node was most recently saved.',"
				+ "`comment` int(11) NOT NULL DEFAULT '0' COMMENT 'Whether comments are allowed on this node: 0 = no, 1 = closed (read only), 2 = open (read/write).',"
				+ "`promote` int(11) NOT NULL DEFAULT '0' COMMENT 'Boolean indicating whether the node should be displayed on the front page.',"
				+ "`sticky` int(11) NOT NULL DEFAULT '0' COMMENT 'Boolean indicating whether the node should be displayed at the top of lists in which it appears.',"
				+ "`tnid` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'The translation set id for this node, which equals the node id of the source post in each set.',"
				+ "`translate` int(11) NOT NULL DEFAULT '0' COMMENT 'A boolean indicating whether this translation page needs to be updated.',"
				+ "PRIMARY KEY (`nid`),"
				+ "UNIQUE KEY `vid` (`vid`),"
				+ "KEY `node_changed` (`changed`),"
				+ "KEY `node_created` (`created`),"
				+ "KEY `node_frontpage` (`promote`,`status`,`sticky`,`created`),"
				+ "KEY `node_status_type` (`status`,`type`,`nid`),"
				+ "KEY `node_title_type` (`title`,`type`(4)),"
				+ "KEY `node_type` (`type`(4)),"
				+ "KEY `uid` (`uid`),"
				+ "KEY `tnid` (`tnid`),"
				+ "KEY `translate` (`translate`),"
				+ "KEY `language` (`language`)"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='The base table for nodes.' /*#dve BROADCAST DISTRIBUTE */"));
		tests.add(new StatementMirrorProc("CREATE TABLE `taxonomy_index` ("
				+ "`nid` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'The node.nid this record tracks.',"
				+ "`tid` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'The term ID.',"
				+ "`sticky` tinyint(4) DEFAULT '0' COMMENT 'Boolean indicating whether the node is sticky.',"
				+ "`created` int(11) NOT NULL DEFAULT '0' COMMENT 'The Unix timestamp when the node was created.',"
				+ "KEY `term_node` (`tid`,`sticky`,`created`),"
				+ "KEY `nid` (`nid`)"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Maintains denormalized information about node/term...' /*#dve RANDOM DISTRIBUTE */"));
		tests.add(new StatementMirrorProc("CREATE TABLE `taxonomy_vocabulary` ("
				+ "`vid` int(10) unsigned NOT NULL COMMENT 'Primary Key: Unique vocabulary ID.',"
				+ "`name` varchar(255) NOT NULL DEFAULT '' COMMENT 'Name of the vocabulary.',"
				+ "`machine_name` varchar(255) NOT NULL DEFAULT '' COMMENT 'The vocabulary machine name.',"
				+ "`description` longtext COMMENT 'Description of the vocabulary.',"
				+ "`hierarchy` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT 'The type of hierarchy allowed within the vocabulary. (0 = disabled, 1 = single, 2 = multiple)',"
				+ "`module` varchar(255) NOT NULL DEFAULT '' COMMENT 'The module which created the vocabulary.',"
				+ "`weight` int(11) NOT NULL DEFAULT '0' COMMENT 'The weight of this vocabulary in relation to other vocabularies.',"
				+ "PRIMARY KEY (`vid`),"
				+ "UNIQUE KEY `machine_name` (`machine_name`),"
				+ "KEY `list` (`weight`,`name`)"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Stores vocabulary information.' /*#dve BROADCAST DISTRIBUTE */"));
		
		tests.add(new StatementMirrorFun("SELECT taxonomy_term_data.tid AS tid, taxonomy_term_data.name AS taxonomy_term_data_name, taxonomy_term_data.vid AS taxonomy_term_data_vid, taxonomy_vocabulary.machine_name AS taxonomy_vocabulary_machine_name FROM taxonomy_term_data taxonomy_term_data INNER JOIN node node_taxonomy_term_data ON (SELECT nodeINNER.nid AS nidINNER FROM node nodeINNER LEFT JOIN taxonomy_index taxonomy_indexINNER ON nodeINNER.nid = taxonomy_indexINNER.nid LEFT JOIN taxonomy_term_data taxonomy_term_data_nodeINNER ON taxonomy_indexINNER.tid = taxonomy_term_data_nodeINNER.tid WHERE (( (taxonomy_term_data_nodeINNER.tid = taxonomy_term_data.tid ) )) ORDER BY nodeINNER.nid DESC LIMIT 1 OFFSET 0) = node_taxonomy_term_data.nid LEFT JOIN taxonomy_vocabulary taxonomy_vocabulary ON taxonomy_term_data.vid = taxonomy_vocabulary.vid WHERE (( (taxonomy_vocabulary.machine_name IN  ('blog_category')) ))"));

		runTest(tests);
	}
}
