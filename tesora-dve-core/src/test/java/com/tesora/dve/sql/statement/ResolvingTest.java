// OS_STATUS: public
package com.tesora.dve.sql.statement;


import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.TestName;

// the point of these tests is to ensure we resolve correctly
public class ResolvingTest extends TransientSchemaTest {

	public ResolvingTest() {
		super("ResolvingTest");
	}
	
	@Test
	public void testPE221A() throws Throwable {
		SchemaContext sc = buildSchema(TestName.MULTI,
				"create table t1 (a int, b int)",
				"create table t2 (a int, b int)");
		List<Statement> stmts = parse(sc, "select t1.a, t2.b from t1, t2 where t1.b = t2.a order by a,b");
		// should ensure the various bits refer to the right thing
		// but actually if we just resolve at all - that's good enough
		assertEquals(stmts.size(), 1);
	}

	@Test
	public void testPE221B() throws Throwable {
		SchemaContext sc = buildSchema(TestName.MULTI,
				"create table t1 (a int, b int)",
				"create table t2 (a int, b int)");
		List<Statement> stmts = parse(sc, "select t1.* from t1, t2 where t1.b = t2.a order by a, b");
		// should ensure the various bits refer to the right thing
		// but actually if we just resolve at all - that's good enough
		assertEquals(stmts.size(), 1);
	}
	
	
	@Test
	public void testCharsetHint() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table cshint (`id` int, `junk` char(1))");
		String sql = "select id, _utf8'on the projection' from cshint where junk = _utf8'a' and junk not in (_utf8'x', _latin1'y', _bigeasy'z')";
		List<Statement> stmts = parse(db, sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT `cshint`.`id`,_utf8'on the projection' FROM `cshint` WHERE `cshint`.`junk` = _utf8'a' and `cshint`.`junk` NOT IN ( _utf8'x',_latin1'y',_bigeasy'z' )",ss.getSQL(db));
	}
	
	@Test
	public void testComments() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table foo (id int)");
		parse(db,"-- Server version      5.1.58-log");
	}

	@Test
	public void testSelectOptions() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table foo (id int)");
		List<Statement> stmts = parse(db,"select sql_no_cache distinct * from foo");
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT DISTINCT SQL_NO_CACHE * FROM `foo`",ss.getSQL(db));
	}
	
	@Test
	public void testPE451() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table `days` (`id` int, `months` varchar(32), `years` varchar(32))");
		List<Statement> stmts = parse(db,"select * from days");
		SelectStatement ss = (SelectStatement)stmts.get(0);
		assertEquals("SELECT * FROM `days`",ss.getSQL(db));
	}
	
	@Test
	public void testPE338() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe338 (`id` int) broadcast distribute");
		List<Statement> stmts = parse(db,"select cast(1 as unsigned)");
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT cast(1 as unsigned)",ss.getSQL(db));
		stmts = parse(db,"select cast(1-2 as unsigned)");
		ss = (SelectStatement)stmts.get(0);
		assertEquals("SELECT cast(1 - 2 as unsigned)",ss.getSQL(db));
		
	}
	
	@Test
	public void testModulo() throws Throwable {
		SchemaContext db= buildSchema(TestName.MULTI,
				"create table argh (`id` int auto_increment, `fid` int, `sid` int, primary key (`id`)) broadcast distribute");
		List<Statement> stmts = parse(db,"select * from argh where (fid % 2) = 0 order by id");
		SelectStatement ss = (SelectStatement)stmts.get(0);
		assertEquals("SELECT * FROM `argh` WHERE  (`argh`.`fid` % 2)  = 0 ORDER BY `argh`.`id` ASC",ss.getSQL(db));
	}
	
	@Test
	public void testPE679() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table catcher (`id` int, `fid` int, primary key (`id`)) broadcast distribute",
				"create table rye (`id` int, `fid` int, primary key (`id`)) broadcast distribute",
				"create table wheat (`id` int, `fid` int, primary key (`id`)) broadcast distribute");
		String sql = "insert into catcher (`id`, `fid`) select r.id, w.fid from rye r inner join wheat w on r.id = w.id on duplicate key update `id` = values(`id`)";
		List<Statement> stmts = parse(db,sql);
		InsertIntoSelectStatement iiss = (InsertIntoSelectStatement) stmts.get(0);
		assertEquals("INSERT INTO `catcher` (`catcher`.`id`,`catcher`.`fid`) SELECT r.`id`,w.`fid` FROM `rye` AS r INNER JOIN `wheat` AS w ON r.`id` = w.`id` ON DUPLICATE KEY UPDATE `catcher`.`id` = values( `catcher`.`id` ) ",
				iiss.getSQL(db));
	}	
	
	@Ignore
	@Test
	public void testPE716() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A (`id` int, `fid` int, primary key (`id`)) random distribute",
				"create table B (`id` int, `fid` int, primary key (`id`)) random distribute");
		String sql =
				"(select id, fid from A where id = 15) union (select id, fid from B where id = 22) order by 2";
		List<Statement> stmts = parse(db,sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		System.out.println(ss.getSQL(db));
	}
	
	@Test
	public void testPE718() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A (`id` int, `fid` int)",
				"create table B (`id` int, `fid` int)");
		String sql = "select a.id, b.id from A a cross join B b";
		List<Statement> stmts = parse(db,sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT a.`id`,b.`id` FROM `A` AS a INNER JOIN `B` AS b",
				ss.getSQL(db));
	}
	
	@Test
	public void testPE720() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table wt (`id` int, `a` int, `b` int, `c` int, `d` int, `e` int)");
		String sql = "select cast(id as signed), cast(b as signed integer), cast(c as unsigned), cast(d as unsigned integer), "
				+ "cast(e as char), cast(id as char(15)), cast(a as decimal(12,4)), cast(b as binary(15)) from wt";
		List<Statement> stmts = parse(db,sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT cast(`wt`.`id` as signed),cast(`wt`.`b` as signed integer),cast(`wt`.`c` as unsigned),cast(`wt`.`d` as unsigned integer),cast(`wt`.`e` as char),cast(`wt`.`id` as char(15)),cast(`wt`.`a` as decimal(12,4)),cast(`wt`.`b` as binary(15)) FROM `wt`",
				ss.getSQL(db));
	}

	@Test
	public void testPE860() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table `node` (`nid` int, `type` varchar(32), `title` varchar(255), `status` int, primary key (`nid`))",
				"create table `taxonomy_term_data` (`tid` int, `vid` int, `name` varchar(255), primary key (`tid`))",
				"create table `taxonomy_index` (`nid` int, `tid` int, key (`nid`))",
				"create table `taxonomy_vocabulary` (`vid` int, `machine_name` varchar(255), primary key (`vid`))");
		String sql = "SELECT DISTINCT "
				+"taxonomy_term_data_node.name AS taxonomy_term_data_node_name, "
				+"taxonomy_term_data_node.vid AS taxonomy_term_data_node_vid, "
				+"taxonomy_term_data_node.tid AS taxonomy_term_data_node_tid, "
				+"taxonomy_term_data_node__taxonomy_vocabulary.machine_name AS taxonomy_term_data_node__taxonomy_vocabulary_machine_name, "
				+"node.nid AS nid, node.title AS node_title, 'node' AS field_data_field_premium_node_entity_type, "
				+"'node' AS field_data_field_channel_node_entity_type, 'node' AS field_data_field_geoblocked_node_entity_type, "
				+"'node' AS field_data_field_has_video_episodes_node_entity_type "
				+"FROM node node INNER JOIN "
				+"  (SELECT td.*, tn.nid AS nid" 
				+"   FROM taxonomy_term_data td INNER JOIN taxonomy_vocabulary tv ON td.vid = tv.vid "
				+"   INNER JOIN taxonomy_index tn ON tn.tid = td.tid WHERE (tv.machine_name IN ('alphabetical')) "
				+"  ) taxonomy_term_data_node ON node.nid = taxonomy_term_data_node.nid "
				+"  LEFT JOIN taxonomy_vocabulary taxonomy_term_data_node__taxonomy_vocabulary ON taxonomy_term_data_node.vid = taxonomy_term_data_node__taxonomy_vocabulary.vid "
				+"WHERE (( (node.status = '1') AND (node.type IN ('series')) )) "
				+"ORDER BY taxonomy_term_data_node_name ASC, node_title ASC ";
		List<Statement> stmts = parse(db,sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT DISTINCT taxonomy_term_data_node.`name` AS taxonomy_term_data_node_name,taxonomy_term_data_node.`vid` AS taxonomy_term_data_node_vid,taxonomy_term_data_node.`tid` AS taxonomy_term_data_node_tid,taxonomy_term_data_node__taxonomy_vocabulary.`machine_name` AS taxonomy_term_data_node__taxonomy_vocabulary_machine_name,node.`nid` AS nid,node.`title` AS node_title,'node' AS field_data_field_premium_node_entity_type,'node' AS field_data_field_channel_node_entity_type,'node' AS field_data_field_geoblocked_node_entity_type,'node' AS field_data_field_has_video_episodes_node_entity_type FROM `node` AS node INNER JOIN ( SELECT td.*,tn.`nid` AS nid FROM `taxonomy_term_data` AS td INNER JOIN `taxonomy_vocabulary` AS tv ON td.`vid` = tv.`vid` INNER JOIN `taxonomy_index` AS tn ON tn.`tid` = td.`tid` WHERE  (tv.`machine_name` IN ( 'alphabetical' )) ) AS taxonomy_term_data_node ON node.`nid` = taxonomy_term_data_node.nid LEFT OUTER JOIN `taxonomy_vocabulary` AS taxonomy_term_data_node__taxonomy_vocabulary ON taxonomy_term_data_node.`vid` = taxonomy_term_data_node__taxonomy_vocabulary.`vid` WHERE  ( (node.`status` = '1')  AND  (node.`type` IN ( 'series' )) )  ORDER BY taxonomy_term_data_node_name ASC, node_title ASC",
				ss.getSQL(db));
	}
	
	@Test
	public void testPE862() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table sbtest (id int unsigned not null auto_increment, k int, c char(120), pad char(60), primary key (id))");
		String sql = 
				"select count(c) from sbtest where id between 50377 and 50377+99";
		List<Statement> stmts = parse(db,sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT count( `sbtest`.`c` )  FROM `sbtest` WHERE `sbtest`.`id` BETWEEN 50377 AND 50377 + 99",
				ss.getSQL(db));
	}
	
	@Test
	public void testPE958() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table node (nid int, created timestamp, primary key (nid))");
		String sql =
				"select count(*) from node where created between unix_timestamp()-(30*60) and unix_timestamp()-(15*60)";
		List<Statement> stmts = parse(db,sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT count( * )  FROM `node` WHERE `node`.`created` BETWEEN unix_timestamp(  )  -  (30 * 60)  AND unix_timestamp(  )  -  (15 * 60) ",
				ss.getSQL(db));
	}
	
	@Test
	public void testPE986() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe986(id int, ts_added varchar(32), primary key (id))");
		String sql = 
				"select `or`.*, UNIX_TIMESTAMP(or.ts_added) as unix_ts_added from pe986 `or` where or.id = 1";
		
		List<Statement> stmts = parse(db,sql);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		assertEquals("SELECT `or`.*,UNIX_TIMESTAMP( `or`.`ts_added` )  AS unix_ts_added FROM `pe986` AS `or` WHERE `or`.`id` = 1",ss.getSQL(db));
	}
	
	@Test
	public void testPE987() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe987(id int, fid varchar(32), primary key (id))");
		String[] in = new String[] {
				"select id from pe987 where convert(substring(fid,2), signed) > 22",
				"select id from pe987 where convert(substring(fid,2), unsigned) > 22",
				"select id from pe987 where convert(substring(fid,2), signed integer) > 22",
				"select id from pe987 where convert(substring(fid,2), unsigned integer) > 22",
				"select convert(fid,date) from pe987",
				"select convert(fid,datetime) from pe987",
				"select convert(fid,time) from pe987",
				"select convert(fid,binary) from pe987",
				"select convert(fid,binary(2)) from pe987",
				"select convert(id,char) from pe987",
				"select convert(id,char(5)) from pe987",
				"select convert(id,decimal) from pe987",
				"select convert(id,decimal(5)) from pe987",
				"select convert(id,decimal(5,2)) from pe987"
		};
		String[] out = new String[] {
				"SELECT `pe987`.`id` FROM `pe987` WHERE CONVERT( substring( `pe987`.`fid`,2 ) ,SIGNED INTEGER )  > 22",
				"SELECT `pe987`.`id` FROM `pe987` WHERE CONVERT( substring( `pe987`.`fid`,2 ) ,UNSIGNED INTEGER )  > 22",
				"SELECT `pe987`.`id` FROM `pe987` WHERE CONVERT( substring( `pe987`.`fid`,2 ) ,SIGNED INTEGER )  > 22",
				"SELECT `pe987`.`id` FROM `pe987` WHERE CONVERT( substring( `pe987`.`fid`,2 ) ,UNSIGNED INTEGER )  > 22",
				"SELECT CONVERT( `pe987`.`fid`,date )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`fid`,datetime )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`fid`,time )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`fid`,binary )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`fid`,binary(2) )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`id`,char )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`id`,char(5) )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`id`,decimal )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`id`,decimal(5) )  FROM `pe987`",
				"SELECT CONVERT( `pe987`.`id`,decimal(5,2) )  FROM `pe987`"
		};
		
		for(int i = 0; i < in.length; i++) {
			String isql = in[i];
			List<Statement> stmts = parse(db,isql);
			SelectStatement ss = (SelectStatement) stmts.get(0);
			String osql = ss.getSQL(db);
			String esql = out[i];
			if (esql == null)
				System.out.println(osql);
			else
				assertEquals(esql,osql);
		}
		
	}
	
}


