// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.TestName;

public class MultiColumnDistributionKeyTransformTest extends TransformTest {

	public MultiColumnDistributionKeyTransformTest() {
		super("MultiColumnDistributionKeyTransformTest");
	}

	@Test
	public void testMultiColumnDistributionKeyRouting() throws Throwable {
		final SchemaContext db = buildSchema(TestName.MULTI,
				"CREATE RANGE IF NOT EXISTS mcdkr_test_range (int, int) PERSISTENT GROUP g1",
				"CREATE TABLE mcdkr_test (id INT NOT NULL, fid INT NOT NULL, value INT) range distribute on (`id`, `fid`) using mcdkr_test_range");
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"SELECT value FROM mcdkr_test t WHERE (t.id, t.fid) IN ((1, 2))",
				SelectStatement.class,
				bes(
				new ProjectingExpectedStep(
						"SELECT t.`value` AS tv2_3 FROM `mcdkr_test` AS t WHERE (t.`id`, t.`fid` ) IN ( (1, 2 ) )",
						group)
				));
	}
}
