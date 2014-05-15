// OS_STATUS: public
package com.tesora.dve.sql;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.raw.RawPlanBuilder;
import com.tesora.dve.sql.raw.jaxb.GroupScaleType;
import com.tesora.dve.sql.raw.jaxb.LiteralType;
import com.tesora.dve.sql.raw.jaxb.ModelType;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class RawPlanTest extends SchemaTest {

	
	private static final ProjectDDL testDDL =
		new PEDDL("rdb", 
				new StorageGroupDDL("prdt",2,"pg"),
				"database");
	
	@BeforeClass
	public static void setup() throws Throwable {
		projectSetup(testDDL);
		bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource conn = new ProxyConnectionResource();
		testDDL.create(conn);
		conn.execute("create range openrange (int) persistent group " + testDDL.getPersistentGroup().getName());
		String body = "(id int, fid int, sid int, primary key (id))";
		conn.execute("create table R " + body + " range distribute on (id) using openrange");
		conn.execute("create table B " + body + " broadcast distribute");
		conn.execute("create table A " + body + " random distribute");
		conn.execute("create table S " + body + " static distribute on (id)");
		StringBuilder values = new StringBuilder();
		int rs = 100;
		for(int i = 1; i <= rs; i++) {
			if (i > 1)
				values.append(",");
			values.append("(").append(i).append(",").append(rs - i).append(",").append(rs % i).append(")");
		}
		String[] tabs = new String[] { "B", "R", "A", "S" };
		for(String s : tabs)
			conn.execute("insert into " + s + " (id,fid,sid) values " + values.toString());
		conn.disconnect();
		conn = null;
	}


	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
		conn.execute("use " + testDDL.getDatabaseName());
		conn.execute("alter dve set raw_plan_cache_limit = 0");		
	}
	
	@After
	public void after() throws Throwable {
		conn.execute("alter dve set raw_plan_cache_limit = 0");		
		conn.disconnect();
		conn = null;
	}
	
	ProxyConnectionResource conn = null;

	private static final String[] examples = new String[] {
		"select * from A where id = 15 and fid = 22 and sid < 41 order by id limit 12",
		"select a.id, b.id from A a inner join R b on a.fid = b.fid where b.sid = 22",
		"select id from S where sid in (37,38,39,40,41,42,43,44,45,46,47,48,49) order by fid asc",
		"select max(a.sid), min(s.sid), convert(avg(r.sid),unsigned), (a.id + s.fid + r.sid)/2 gb from S s left outer join A a on s.id = a.id inner join R r on a.id = r.id where a.fid in (99,97,95,94,93) and s.id in (1,3,5,7,9) and r.id in (1,3,5,7,9) group by gb order by gb asc"
	};
	
	@Test
	public void testRawFormat() throws Throwable {
		for(String s : examples) {
			SchemaTest.echo(s);
			SchemaTest.echo(conn.printResults("explain " + s));
			SchemaTest.echo(conn.printResults("explain raw=1 " + s));
		}
	}	
	
	@Test
	public void testRawRoundtrip() throws Throwable {
		for(String s : examples) {
			SchemaTest.echo(s);
			ResourceResponse rr = conn.fetch("explain raw=1 " + s);
			List<ResultRow> results = rr.getResults();
			assertEquals("should have one row",1,results.size());
			String xml = (String) results.get(0).getResultColumn(1).getColumnValue();
			String create = "create raw plan rpt database " + testDDL.getDatabaseName() + " comment 'raw plan test' xml='" + xml + "'";
			conn.execute(create);
			conn.assertResults("show warnings", br(nr,ignore,ignore,"Too many enabled raw plans.  Found 1 but can only cache 0"));
			conn.assertResults("show raw plans", br(nr,ignore,"YES",ignore,ignore,xml));
			conn.execute("alter raw plan rpt set enabled = false");
			conn.execute("drop raw plan rpt");
		}
	}
	
	@Test
	public void testSimpleRawPlanHit() throws Throwable {
		RawPlanBuilder builder = new RawPlanBuilder()
			.withInSQL("select id from S where sid in (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13) order by fid asc");
		for(int i = 1; i < 14; i++)
			builder.withParameter("p" + i,LiteralType.INTEGRAL);
		builder.withDynamicGroup("dg0", GroupScaleType.AGGREGATE)
			.withRedistStep("select id as first_col, fid as order_col from S where sid in (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13)",
					"pg", ModelType.STATIC,
					"first",true,"dg0",ModelType.STATIC)
			.withFinalProjectingStep("select first_col from first order by order_col desc", "dg0", ModelType.STATIC);
		String xml = builder.toXML();
		Object[] forward = br(nr,63,nr,62,nr,61,nr,60,nr,59,nr,58,nr,57,nr,56,nr,55,nr,54,nr,53,nr,52,nr,51);
		Object[] reverse = br(nr,51,nr,52,nr,53,nr,54,nr,55,nr,56,nr,57,nr,58,nr,59,nr,60,nr,61,nr,62,nr,63); 
		conn.assertResults(examples[2],forward);
		conn.execute("create raw plan rpt database " + testDDL.getDatabaseName() + " xml='" + xml + "' enabled=false");
		conn.assertResults("show raw plans",br(nr,"rpt","NO","select id from S where sid in (?,?,?,?,?,?,?,?,?,?,?,?,?) order by fid asc",null,ignore));
		conn.execute("alter raw plan rpt set enabled=true");
		conn.assertResults("show raw plans",br(nr,"rpt","YES","select id from S where sid in (?,?,?,?,?,?,?,?,?,?,?,?,?) order by fid asc",null,ignore));
		conn.execute("alter dve set raw_plan_cache_limit = 10");
		conn.assertResults(examples[2],reverse);
		conn.assertResults("show raw plans",br(nr,"rpt","YES","select id from S where sid in (?,?,?,?,?,?,?,?,?,?,?,?,?) order by fid asc",null,ignore));
		for(int i = 0; i < 3; i++) {
			conn.execute("alter raw plan rpt set enabled=false");
			conn.assertResults("show raw plans",br(nr,"rpt","NO","select id from S where sid in (?,?,?,?,?,?,?,?,?,?,?,?,?) order by fid asc",null,ignore));
			conn.assertResults(examples[2],forward);
			conn.execute("alter raw plan rpt set enabled=true");
			conn.assertResults("show raw plans",br(nr,"rpt","YES","select id from S where sid in (?,?,?,?,?,?,?,?,?,?,?,?,?) order by fid asc",null,ignore));
			conn.assertResults(examples[2],reverse);
		}
	}

	@Test
	public void testComplexRawPlanHit() throws Throwable {
//		System.out.println(conn.printResults(examples[3]));
		conn.assertResults(examples[3],br(nr,0,0,0L,ignore,nr,1,1,1L,ignore,nr,2,2,2L,ignore));
		RawPlanBuilder builder = new RawPlanBuilder();
		builder.withInSQL("select max(a.sid), min(s.sid), convert(avg(r.sid),unsigned), (a.id + s.fid + r.sid)/@p0 gb from S s left outer join A a on s.id = a.id inner join R r on a.id = r.id where a.fid in (@p1,@p2,@p3,@p4,@p5) and s.id in (@p6,@p7,@p8,@p9,@p10) and r.id in (@p11,@p12,@p13,@p14,@p15) group by gb order by gb asc");
		for(int i = 0; i < 16; i++)
			builder.withParameter("p" + i,LiteralType.INTEGRAL);
		builder.withDynamicGroup("dg0", GroupScaleType.MEDIUM);
		builder.withDynamicGroup("dg1", GroupScaleType.AGGREGATE);
		builder.withRedistStep("SELECT s.sid AS ss2_4,s.fid AS sf1_5,s.id AS si0_6 FROM `S` AS s WHERE  (s.`id` = @p6 OR s.`id` = @p7 OR s.`id` = @p8 OR s.`id` = @p9)",
				"pg",ModelType.STATIC,"temp15",true,"pg",ModelType.BROADCAST);
		builder.withRedistStep("SELECT r.sid AS rs2_3,r.id AS ri0_4 FROM `R` AS r WHERE  (r.`id` = @p11 OR r.`id` = @p12 OR r.`id` = @p13 OR r.`id` = @p14)", 
				"pg", ModelType.RANGE, "temp16", true, "pg", ModelType.BROADCAST);
		builder.withRedistStep("SELECT max( a.sid )  AS func_24,min( temp15.ss2_4 )  AS func_25,sum( temp16.rs2_3 )  AS func_26,COUNT( temp16.rs2_3 )  AS func_27, (a.`id` + temp15.sf1_5 + temp16.rs2_3)  / @p0 AS func_28 FROM temp15 INNER JOIN `A` AS a ON temp15.si0_6 = a.`id` INNER JOIN temp16 ON a.`id` = temp16.ri0_4 WHERE a.`fid` in ( @p1,@p2,@p3,@p4) GROUP BY func_28 ASC",
				"pg", ModelType.RANDOM, "temp17", true, "dg0",  ModelType.STATIC, "func_28");
		builder.withRedistStep("SELECT max( temp17.func_24 )  AS func,min( temp17.func_25 )  AS func_12, convert(  (sum( temp17.func_26 )  / sum( temp17.func_27 ) ), unsigned integer)  AS func_13,temp17.func_28 AS t6f4_14 FROM temp17 GROUP BY t6f4_14 ASC",
				"dg0",ModelType.STATIC,"temp18",true,"dg1",ModelType.STATIC);
		builder.withFinalProjectingStep("SELECT temp18.func AS t7f0,temp18.func_12 AS t7f1,temp18.func_13 AS t7f2,temp18.t6f4_14 AS t7t3 FROM temp18 ORDER BY t7t3 ASC",
				"dg1",ModelType.STATIC);
		String xml = builder.toXML();
		conn.execute("create raw plan crp database " + testDDL.getDatabaseName() + " xml='" + xml + "' enabled=false");
		conn.execute("alter dve set raw_plan_cache_limit = 10");
		conn.execute("alter raw plan crp set enabled=true");
		conn.assertResults(examples[3], br(nr,0,0,0L,ignore,nr,1,1,1L,ignore));
		conn.execute("drop raw plan crp");
	}
	
}
