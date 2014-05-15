// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.util.TestName;

/**
 * Tests to make sure the parser handles syntax that is actually ignored or handled elsewhere
 * 
 * @author peter
 *
 */
public class EmptyStatementTransformTest extends TransformTest {

	// we don't need a schema
	private static final String[] schema = new String[] {};

	public EmptyStatementTransformTest() {
		super("EmptyStatementTransformTest");
	}

    @Test
    public void testPE1152_GrantProcessIgnored() throws Exception {
        SchemaContext db = buildSchema(TestName.MULTI, schema);
        stmtTest(db, "GRANT process ON *.* TO `f5_db_user`@`%` IDENTIFIED BY 'S'", EmptyStatement.class, null);
        stmtTest(db, "GRANT process ON *.* TO `f5_db_user`@`%`", EmptyStatement.class, null);
    }

}
