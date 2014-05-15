// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.session.LoadDataInfileStatement;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.util.TestName;

public class LoadDataInfileTransformTest extends TransformTest {

	public LoadDataInfileTransformTest() {
		super("EMPTY LOAD DATA INFILE");
	}

	private static final String[] leftySchema = new String[] { "create table `A` (`id` int unsigned not null, `col1` varchar(50) not null, `id2` int, `col2` varchar(10) ) random distribute", };

	@Ignore
	@Test
	public void basicTest() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI, leftySchema);
//		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"LOAD DATA LOCAL INFILE 'd:/vmshare/loaddata.dat' IGNORE INTO TABLE `a` FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' (`id`, `field`)",
				LoadDataInfileStatement.class,
				bes(new ExpectedStep(EmptyExecutionStep.class, null, projectName)));
	}
}
