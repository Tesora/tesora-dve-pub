// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.TestName;

public class SessionRewriteTransformTest extends TransformTest {

	public SessionRewriteTransformTest() {
		super("SessionRewrite");
		// TODO Auto-generated constructor stub
	}

	// our tiny little schema
	private static final String[] schema = new String[] {
		"create table A (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) static distribute on (`id`);",
		"create table B (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) broadcast distribute;",
		"create table R (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) random distribute;",
	};

	@Test
	public void testSimpleDatabase() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db, "select database()",
				SelectStatement.class,
				bes(new AdhocResultsSessionExpectedStep()));
	}
	
	@Test
	public void testComplexTransientDatabase() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db, "select concat('my',database())",
				SelectStatement.class,
				bes(new ProjectingExpectedStep("SELECT concat( 'my','mydb' )  AS func", null)));	
	}
	
	@Test
	public void testPE351() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI);
		String sql = "select case 1.0 when 0.1 then 'a' when 1.0 then 'b' else 'c' end";
		stmtTest(db,sql,
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT CASE 1.0 WHEN 0.1 THEN 'a' WHEN 1.0 THEN 'b' ELSE 'c' END AS casex",
							null)
					));
	}

	@Test
	public void testPE730() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,"select @@version_comment limit 1",
				SelectStatement.class,
				bes(new AdhocResultsSessionExpectedStep()));
	}
	

}
