// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.util.TestName;

public class DeleteTransformTest extends TransformTest {

	public DeleteTransformTest() {
		super("DeleteTransformTest");
	}
	
	@Test
	public void testPE610() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe610a (`id` int, `fid` int, primary key (`id`)) random distribute",
				"create table pe610b (`id` int, `fid` int, primary key (`id`)) broadcast distribute");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"delete p from pe610a p inner join pe610b q on p.fid = q.fid where q.id = 15",
				DeleteStatement.class,
				bes(
						new DeleteExpectedStep(group,
						"DELETE p FROM `pe610a` AS p INNER JOIN `pe610b` AS q ON p.`fid` = q.`fid` WHERE q.`id` = 15")
					));
		stmtTest(db,
				"delete pe610a from pe610a inner join pe610b on pe610a.fid = pe610b.fid where pe610b.id = 15",
				DeleteStatement.class,
				bes(
						new DeleteExpectedStep(group,
						"DELETE `pe610a` FROM `pe610a` INNER JOIN `pe610b` ON `pe610a`.`fid` = `pe610b`.`fid` WHERE `pe610b`.`id` = 15")
					));
	}
	
}
