// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.TestName;

public class PreparedStatementTransformTest extends TransformTest {

	public PreparedStatementTransformTest() {
		super("PreparedStatementTransformTest");
	}
	
	@Test
	public void testDistKeyA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, 
				"create table tdka (`id` int, `sid` int, `oth` varchar(32), primary key (`id`)) static distribute on (`id`)");
		prepareTest(db,
				"select * from tdka where id = ?",
				1,
				null);
	}

	@Test
	public void testDistKeyB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, 
				"create table tdkb (`id` int, `sid` int, `oth` varchar(32), primary key (`id`)) static distribute on (`id`)");
		prepareTest(db,
				"select * from tdkb where id in (?,?,?,?,?)",
				5,
				null);
	}

}
