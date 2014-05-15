// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class UpdateIgnoreTransformTest extends TransformTest {

	
	public UpdateIgnoreTransformTest() {
		super("UpdateIgnoreTransformTest");
	}

	private static final String[] schema = new String[] {
			"create table A (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int) static distribute on (`id`);",
			"create table B (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int) broadcast distribute;",
			"create table C (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int) random distribute;",
			"create table D (`id` int unsigned not null primary key, `desc` varchar(50), `flags` int) range distribute on (`id`) using openrange",
			"create table pe771 (`id` int unsigned not null auto_increment, `code` tinyint unsigned not null, `name` char(20) not null, primary key (`id`), key (`code`), unique (`name`)) engine=MyISAM RANGE DISTRIBUTE ON (`id`) USING openrange"
	};
	
	@Test
	public void testUpdateIgnoreBroadcast() throws Exception {
		final SchemaContext db = buildSchema(TestName.MULTI, schema);
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"UPDATE IGNORE B SET id=1, desc='Hello, World!', flags=0 WHERE id > 3",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(group,
						"UPDATE IGNORE `B` SET `B`.`id` = 1,`B`.`desc` = 'Hello, World!',`B`.`flags` = 0 WHERE `B`.`id` > 3")
				));
	}

	@Test
	public void testUpdateIgnoreRandom() throws Exception {
		final SchemaContext db = buildSchema(TestName.MULTI, schema);
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"UPDATE IGNORE C SET id=1, desc='Hello, World!', flags=0 WHERE id > 3",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(group,"UPDATE IGNORE `C` SET `C`.`id` = 1,`C`.`desc` = 'Hello, World!',`C`.`flags` = 0 WHERE `C`.`id` > 3")
				));
	}

	@Test
	public void testUpdateIgnoreStatic() throws Exception {
		final SchemaContext db = buildSchema(TestName.MULTI, schema);
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"UPDATE IGNORE A SET id=1, desc='Hello, World!', flags=0 WHERE id > 3",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT `A`.`id` AS A1i0_4,`A`.`desc` AS A1d1_5,`A`.`flags` AS A1f2_6 FROM `A` WHERE `A`.`id` > 3 OR `A`.`id` = 1 FOR UPDATE",
								group, "temp1", TransientExecutionEngine.AGGREGATION, BroadcastDistributionModel.MODEL_NAME, new String[] { "A1i0_4" }),
						new FilterExpectedStep("LateBindingUpdateCounter", new UpdateExpectedStep(null,
								"UPDATE IGNORE temp1 SET temp1.A1i0_4 = 1,temp1.A1d1_5 = 'Hello, World!',temp1.A1f2_6 = 0")),
						new DeleteExpectedStep(group,
								"DELETE `A` FROM `A` WHERE `A`.`id` > 3 OR `A`.`id` = 1"),
						new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT temp1.A1i0_4 AS `id`,temp1.A1d1_5 AS `desc`,temp1.A1f2_6 AS `flags` FROM temp1",
								TransientExecutionEngine.AGGREGATION, "A", group, StaticDistributionModel.MODEL_NAME, new String[] {})
				));
	}

	@Test
	public void testUpdateIgnoreStaticNonDvUpdate() throws Exception {
		final SchemaContext db = buildSchema(TestName.MULTI, schema);
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"UPDATE IGNORE A SET desc='Hello, World!', flags=0 WHERE id > 3",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(group,"UPDATE IGNORE `A` SET `A`.`desc` = 'Hello, World!',`A`.`flags` = 0 WHERE `A`.`id` > 3")
				));
	}

	@Test
	public void testUpdateIgnoreRange() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI, schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"UPDATE IGNORE D SET id=1, desc='Hello, World!', flags=0 WHERE id > 3",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT `D`.`id` AS D1i0_4,`D`.`desc` AS D1d1_5,`D`.`flags` AS D1f2_6 FROM `D` WHERE `D`.`id` > 3 OR `D`.`id` = 1 FOR UPDATE",
								group, "temp1", TransientExecutionEngine.AGGREGATION, BroadcastDistributionModel.MODEL_NAME, new String[] { "D1i0_4" }),
						new FilterExpectedStep("LateBindingUpdateCounter", new UpdateExpectedStep(null,
						"UPDATE IGNORE temp1 SET temp1.D1i0_4 = 1,temp1.D1d1_5 = 'Hello, World!',temp1.D1f2_6 = 0")),
						new DeleteExpectedStep(group,
						"DELETE `D` FROM `D` WHERE `D`.`id` > 3 OR `D`.`id` = 1"),
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT temp1.D1i0_4 AS `id`,temp1.D1d1_5 AS `desc`,temp1.D1f2_6 AS `flags` FROM temp1",
								TransientExecutionEngine.AGGREGATION, "D", group, RangeDistributionModel.MODEL_NAME, new String[] {})
					)
				);
	}

	@Test
	public void testUpdateIgnoreRangeNonDvUpdate() throws Exception {
		final SchemaContext db = buildSchema(TestName.MULTI, schema);
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"UPDATE IGNORE D SET desc='Hello, World!', flags=0 WHERE id > 3",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(group,
						"UPDATE IGNORE `D` SET `D`.`desc` = 'Hello, World!',`D`.`flags` = 0 WHERE `D`.`id` > 3")
				));
	}

	@Test
	public void testUpdateIgnoreRangePE771() throws Exception {
		final SchemaContext db = buildSchema(TestName.MULTI, schema);
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"UPDATE IGNORE pe771 SET id = 8, name = 'Sinisa' WHERE id < 3",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(
								ExecutionType.SELECT,
								"SELECT `pe771`.`id` AS p1i0_4,`pe771`.`code` AS p1c1_5,`pe771`.`name` AS p1n2_6 FROM `pe771` WHERE `pe771`.`id` < 3 OR `pe771`.`id` = 8 OR `pe771`.`name` = 'Sinisa' FOR UPDATE",
								group, "temp1", TransientExecutionEngine.AGGREGATION, BroadcastDistributionModel.MODEL_NAME, new String[] { "p1i0_4" }),
						new FilterExpectedStep("LateBindingUpdateCounter", new UpdateExpectedStep(null,
								"UPDATE IGNORE temp1 SET temp1.p1i0_4 = 8,temp1.p1n2_6 = 'Sinisa'")),
						new DeleteExpectedStep(group,
								"DELETE `pe771` FROM `pe771` WHERE `pe771`.`id` < 3 OR `pe771`.`id` = 8 OR `pe771`.`name` = 'Sinisa'"),
						new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT temp1.p1i0_4 AS `id`,temp1.p1c1_5 AS `code`,temp1.p1n2_6 AS `name` FROM temp1",
								TransientExecutionEngine.AGGREGATION, "pe771", group, RangeDistributionModel.MODEL_NAME, new String[] {})
				));
	}
}
