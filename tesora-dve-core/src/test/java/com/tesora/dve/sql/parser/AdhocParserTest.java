// OS_STATUS: public
package com.tesora.dve.sql.parser;

import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.TransformTest;
import com.tesora.dve.sql.util.TestName;

public class AdhocParserTest extends TransformTest {

	public AdhocParserTest() {
		super("AdhocParserTest");
	}
	
	@Test
	public void testPE26() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table t1 (a int)");
		parse(db, "select * from t1 where (1 and a)");
		parse(db, "select * from t1 where NOT (1 and a)");		
	}
	
	@Test
	public void testPE1486() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe1486 (`phase` int)");
		parse(db, "select phase from pe1486");
	}
}
