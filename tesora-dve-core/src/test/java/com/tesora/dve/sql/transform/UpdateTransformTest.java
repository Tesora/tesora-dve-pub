package com.tesora.dve.sql.transform;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;
import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.execution.IdentityConnectionValuesMap;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.TestName;
import com.tesora.dve.sql.util.TimestampVariableTestUtils;

public class UpdateTransformTest extends TransformTest {

	
	public UpdateTransformTest() {
		super("UpdateTransformTest");
	}

	// our tiny little schema
	private static final String[] schema = new String[] {
		"create table A (`id` int unsigned not null, `desc` varchar(50), flags int) static distribute on (`id`);",
		"create table B (`id` int unsigned not null, `desc` varchar(50), flags int) broadcast distribute;",
		"create table C (`id` int unsigned not null, `desc` varchar(50), flags int) random distribute;",
		"create table D (`id` int unsigned not null, `desc` varchar(50), flags int) range distribute on (`id`) using openrange"
	};

	@Test
	public void testNonVectA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update A set desc = 'wrong' where id = 1",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `A` SET `A`.`desc` = 'wrong' WHERE `A`.`id` = 1")
							.withFakeKey(buildFakeKey(new Object[] { "id", new Long(1) }))));
	}

	@Test
	public void testNonVectB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update B set desc = 'wrong' where id = 1",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `B` SET `B`.`desc` = 'wrong' WHERE `B`.`id` = 1")));
	}

	@Test
	public void testNonVectC() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update C set desc = 'wrong' where id = 1",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `C` SET `C`.`desc` = 'wrong' WHERE `C`.`id` = 1")));
	}

	
	@Test
	public void testNonVectD() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update D set desc = 'wrong' where id = 1",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `D` SET `D`.`desc` = 'wrong' WHERE `D`.`id` = 1")
							.withFakeKey(buildFakeKey(new Object[] { "id", new Long(1) }))));
	}

	@Test
	public void testSetVectA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update A set id = 15 where desc like '%wrong%'",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
								group, "temp1", TransientExecutionEngine.AGGREGATION, BroadcastDistributionModel.MODEL_NAME,
								new String[] { "A1i0_4" },
								emptyIndexes,
								"SELECT `A`.`id` AS A1i0_4,`A`.`desc` AS A1d1_5,`A`.`flags` AS A1f2_6",
								"FROM `A`",
								"WHERE `A`.`desc` like '%wrong%' FOR UPDATE"
						),
						new FilterExpectedStep("LateBindingUpdateCounter", new UpdateExpectedStep(
								TransientExecutionEngine.AGGREGATION,
								"UPDATE temp1",
								"SET temp1.A1i0_4 = 15",
								"WHERE temp1.A1d1_5 like '%wrong%'"
								)
						),
						new DeleteExpectedStep(
								group,
								"DELETE `A`",
								"FROM `A`",
								"WHERE `A`.`desc` like '%wrong%'"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
								TransientExecutionEngine.AGGREGATION, "A", group, StaticDistributionModel.MODEL_NAME,
								emptyDV,
								"SELECT temp1.A1i0_4 AS `id`,temp1.A1d1_5 AS `desc`,temp1.A1f2_6 AS `flags`",
								"FROM temp1"
						)
								.withExplain(new DMLExplainRecord(DMLExplainReason.COMPLEX_UPDATE_REDIST_TO_TARGET_TABLE))
				));
	}

	@Test
	public void testSetVectB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update B set id = 15 where desc like '%wrong%'",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `B` SET `B`.`id` = 15 WHERE `B`.`desc` like '%wrong%'")));
	}

	@Test
	public void testSetVectC() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update C set id = 15 where desc like '%wrong%'",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `C` SET `C`.`id` = 15 WHERE `C`.`desc` like '%wrong%'")));
	}

	@Test
	public void testSetVectD() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update D set id = 15 where desc like '%wrong%'",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
								group, "temp1", TransientExecutionEngine.AGGREGATION, BroadcastDistributionModel.MODEL_NAME,
								new String[] { "D1i0_4" },
								emptyIndexes,
								"SELECT `D`.`id` AS D1i0_4,`D`.`desc` AS D1d1_5,`D`.`flags` AS D1f2_6",
								"FROM `D`",
								"WHERE `D`.`desc` like '%wrong%' FOR UPDATE"
						),
						new FilterExpectedStep("LateBindingUpdateCounter", new UpdateExpectedStep(
								TransientExecutionEngine.AGGREGATION,
								"UPDATE temp1",
								"SET temp1.D1i0_4 = 15",
								"WHERE temp1.D1d1_5 like '%wrong%'"
								)
						),
						new DeleteExpectedStep(
								group,
								"DELETE `D`",
								"FROM `D`",
								"WHERE `D`.`desc` like '%wrong%'"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
								TransientExecutionEngine.AGGREGATION, "D", group, RangeDistributionModel.MODEL_NAME,
								emptyDV,
								"SELECT temp1.D1i0_4 AS `id`,temp1.D1d1_5 AS `desc`,temp1.D1f2_6 AS `flags`",
								"FROM temp1"
						)
								.withExplain(new DMLExplainRecord(DMLExplainReason.COMPLEX_UPDATE_REDIST_TO_TARGET_TABLE))
				));
		
	}
	
	@Test
	public void testUseVectD() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update D set flags = id + 15 where desc like '%wrong%'",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `D` SET `D`.`flags` = `D`.`id` + 15 WHERE `D`.`desc` like '%wrong%'")));
	}
	
	@Test
	// Repro for PE-108
	public void testPE108a() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update C set flags=flags+1",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `C` SET `C`.`flags` = `C`.`flags` + 1")));
	}
	
	// Repro for PE-108
	@Test
	public void testPE108b() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update C set flags=flags-1",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `C` SET `C`.`flags` = `C`.`flags` - 1")));
	}

	@Test
	public void testDatabase35580A() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update D set `desc`='foo' where id in (select c.id from C c where c.flags = 1)",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
								group, "temp1", group, BroadcastDistributionModel.MODEL_NAME,
								emptyDV,
								emptyIndexes,
								"SELECT c.`id` AS ci0_4",
								"FROM `C` AS c",
								"WHERE c.`flags` = 1"
						),
						new UpdateExpectedStep(
								group,
								"UPDATE `D`, temp1",
								"SET `D`.`desc` = 'foo'",
								"WHERE `D`.`id` = temp1.ci0_4"
						)
				));
	}

	@Test
	public void testUpdateDeleteA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table LS (`id` int, `other` varchar(24), primary key (`id`) ) static distribute on (`id`)",
				"create table RS (`id` int, `other` varchar(24), primary key (`id`) ) static distribute on (`id`)");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update LS set other='update test' where id in (select t.id from RS t where t.id in (1,2))",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
								group, "temp1", group, BroadcastDistributionModel.MODEL_NAME,
								emptyDV,
								emptyIndexes,
								"SELECT t.`id` AS ti0_4",
								"FROM `RS` AS t",
								"WHERE  (t.`id` = 1 OR t.`id` = 2)"
						)
								.withExplain(new DMLExplainRecord(DMLExplainReason.DISTRIBUTION_KEY_MATCHED, null, 2L)),
						new UpdateExpectedStep(
								group,
								"UPDATE `LS`, temp1",
								"SET `LS`.`other` = 'update test'",
								"WHERE `LS`.`id` = temp1.ti0_4"
						)
				));
	}
	
	@Test
	public void testTimestampVariable() throws Throwable {
		int i = 0;
		for (Object[] objects : TimestampVariableTestUtils.getTestValues()) {
			String value = (String)objects[0];
			Boolean nullable = BooleanUtils.toBoolean((Integer)objects[1]);
			String defaultValue = (String)objects[2];
			Boolean onUpdate = BooleanUtils.toBoolean((Integer)objects[3]);
			Boolean expectedInsertTSVarSet = BooleanUtils.toBoolean((Integer)objects[4]);
			Boolean expectedUpdateTSVarSet = BooleanUtils.toBoolean((Integer)objects[5]);
			String tableName = "ts" + i;
			String testDesc = "testTimestampVariable for table " + tableName;
			
			String createTableSQL = TimestampVariableTestUtils.buildCreateTableSQL(tableName, nullable, defaultValue, onUpdate);
			String insertSQL = TimestampVariableTestUtils.buildInsertTestSQL(tableName, value, 1, Integer.toString(i));
			String updateSQL = TimestampVariableTestUtils.buildUpdateTestSQL(tableName, value, 1, Integer.toString(i)+Integer.toString(i));

			if (isNoisy()) {
				SchemaTest.echo(createTableSQL);
				System.out.println(Functional.joinToString(Arrays.asList(objects), ", "));
			}
			
			SchemaContext dbSingle = buildSchema(TestName.SINGLE, createTableSQL);
			tsVarSetTest(dbSingle, insertSQL, InsertIntoValuesStatement.class, expectedInsertTSVarSet, testDesc);
			
			tsVarSetTest(dbSingle, updateSQL, UpdateStatement.class, expectedUpdateTSVarSet, testDesc);
			
			SchemaContext dbMulti = buildSchema(TestName.MULTI, createTableSQL);
			tsVarSetTest(dbMulti, insertSQL, InsertIntoValuesStatement.class, expectedInsertTSVarSet, testDesc);
			tsVarSetTest(dbMulti, updateSQL, UpdateStatement.class, expectedUpdateTSVarSet, testDesc);

			++i;
		}
	}
	
	@Test
	public void testTimestampFunctions() throws Throwable {
		String tableName = "testtsfunc";
		String createTableSQL = TimestampVariableTestUtils.buildCreateTableSQL(tableName, true, "", false);

		String descKern = "testTimestampFunctions test ";
		
		// we want to know if the timestamp family of functions will cause the timestamp variable to be set by our statements
		SchemaContext dbSingle = buildSchema(TestName.SINGLE, createTableSQL);
		tsVarSetTest(dbSingle, "select * from " + tableName + " where ts < current_timestamp", SelectStatement.class, true, descKern + 1);
		tsVarSetTest(dbSingle, "select * from " + tableName + " where unix_timestamp(ts) < 1", SelectStatement.class, false, descKern + 2);
		tsVarSetTest(dbSingle, "select * from " + tableName + " where unix_timestamp(ts) < unix_timestamp()", SelectStatement.class, true, descKern + 3);
		tsVarSetTest(dbSingle, "delete from " + tableName + " where ts < current_timestamp", DeleteStatement.class, true, descKern + 4);
		tsVarSetTest(dbSingle, "delete from " + tableName + " where unix_timestamp(ts) < 1", DeleteStatement.class, false, descKern + 5);
		tsVarSetTest(dbSingle, "delete from " + tableName + " where unix_timestamp(ts) < unix_timestamp()", DeleteStatement.class, true, descKern + 6);
		
		SchemaContext dbMulti = buildSchema(TestName.MULTI, createTableSQL);
		tsVarSetTest(dbMulti, "select * from " + tableName + " where ts < current_timestamp", SelectStatement.class, true, descKern + 7);
		tsVarSetTest(dbMulti, "select * from " + tableName + " where unix_timestamp(ts) < 1", SelectStatement.class, false, descKern + 8);
		tsVarSetTest(dbMulti, "select * from " + tableName + " where unix_timestamp(ts) < unix_timestamp()", SelectStatement.class, true, descKern + 9);
		tsVarSetTest(dbMulti, "delete from " + tableName + " where ts < current_timestamp", DeleteStatement.class, true, descKern + 10);
		tsVarSetTest(dbMulti, "delete from " + tableName + " where unix_timestamp(ts) < 1", DeleteStatement.class, false, descKern + 11);
		tsVarSetTest(dbMulti, "delete from " + tableName + " where unix_timestamp(ts) < unix_timestamp()", DeleteStatement.class, true, descKern + 12);
	}

	protected ExecutionPlan tsVarSetTest(SchemaContext db, String in,
			Class<?> stmtClass, boolean expected, String test) throws Exception {
		List<Statement> stmts = parse(db, in);
		assertEquals(stmts.size(), 1);
		Statement first = stmts.get(0);
		assertInstanceOf(first, stmtClass);
		assertTrue("Expected timestamp variable to be set to " + expected + " in " + test + " for stmt " + first.getSQL(db),
				((DMLStatement) first).getDerivedInfo()
						.doSetTimestampVariable() == expected);
		ExecutionPlan ep = Statement.getExecutionPlan(db,first);
		if (isNoisy()) {
			System.out.println("In: '" + in + "'");
			ep.display(db,new IdentityConnectionValuesMap(db.getValues()),System.out,null);
		}
		return ep;
	}
	
	@Test
	public void testUpdateDecomposition() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"update C set flags=flags",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
								group,
								"UPDATE `C` SET `C`.`flags` = `C`.`flags`")));		
	}
	
		@Test
	public void testPE1425Simple() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);

		stmtTest(db,
				"update D set id = id + 1, flags = flags where id = -1",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,BroadcastDistributionModel.MODEL_NAME,
							new String[] {"D1i0_6" },
							emptyIndexes,
						  "SELECT `D`.`id` AS D1i0_6,`D`.`desc` AS D1d1_7,`D`.`flags` AS D1f2_8,`D`.`id` + 1 AS func_9,`D`.`flags` AS D1f2_10",
						  "FROM `D`",
						  "WHERE `D`.`id` = -1 FOR UPDATE"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.DISTRIBUTION_KEY_MATCHED)),
					new FilterExpectedStep("LateBindingUpdateCounter",new UpdateExpectedStep(
						TransientExecutionEngine.AGGREGATION,
					  "UPDATE temp1",
					  "SET temp1.D1i0_6 = temp1.func_9,temp1.D1f2_8 = temp1.D1f2_8",
					  "WHERE temp1.D1i0_6 = -1"
					)
					),
						new DeleteExpectedStep(
							group,
						  "DELETE `D`",
						  "FROM `D`",
						  "WHERE `D`.`id` = -1"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.AGGREGATION,"D",group,RangeDistributionModel.MODEL_NAME,
							emptyDV,
						  "SELECT temp1.D1i0_6 AS `id`,temp1.D1d1_7 AS `desc`,temp1.D1f2_8 AS `flags`",
						  "FROM temp1"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.COMPLEX_UPDATE_REDIST_TO_TARGET_TABLE))
					)
				);
	}

	@Test
	public void testPE1425Complex() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		
		stmtTest(db,
				"update D set id = (id + 1) * flags - 1, flags = flags where id = -1",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,BroadcastDistributionModel.MODEL_NAME,
							new String[] {"D1i0_6" },
							emptyIndexes,
						  "SELECT `D`.`id` AS D1i0_6,`D`.`desc` AS D1d1_7,`D`.`flags` AS D1f2_8, (`D`.`id` + 1)  * `D`.`flags` - 1 AS func_9,`D`.`flags` AS D1f2_10",
						  "FROM `D`",
						  "WHERE `D`.`id` = -1 FOR UPDATE"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.DISTRIBUTION_KEY_MATCHED)),
					new FilterExpectedStep("LateBindingUpdateCounter",new UpdateExpectedStep(
						TransientExecutionEngine.AGGREGATION,
					  "UPDATE temp1",
					  "SET temp1.D1i0_6 = temp1.func_9,temp1.D1f2_8 = temp1.D1f2_8",
					  "WHERE temp1.D1i0_6 = -1"
					)
					),
						new DeleteExpectedStep(
							group,
						  "DELETE `D`",
						  "FROM `D`",
						  "WHERE `D`.`id` = -1"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.AGGREGATION,"D",group,RangeDistributionModel.MODEL_NAME,
							emptyDV,
						  "SELECT temp1.D1i0_6 AS `id`,temp1.D1d1_7 AS `desc`,temp1.D1f2_8 AS `flags`",
						  "FROM temp1"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.COMPLEX_UPDATE_REDIST_TO_TARGET_TABLE))
					)
				);
	}

	@Test
	public void testPE1476() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);

		stmtTest(db,
				"update D d set d.desc = 'foo' where d.id in (select b.id from B b)",
				UpdateStatement.class,
				bes(
				new UpdateExpectedStep(
						group,
						"UPDATE `D` AS d, `B` AS b",
						"SET d.`desc` = 'foo'",
						"WHERE d.`id` = b.`id`"
				)
				));
	}

	@Test
	public void testMultiModelNested() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);

		stmtTest(db,
				"update D set `desc`='foo' where"
						+ " (id in (select b.id from B b where b.flags in (select c.flags from C c where c.id > 0)) AND"
						+ " (id in (select c.id from C c where c.flags in (select b.flags from B b where b.id > 0))))",
				UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
								group, "temp1", group, BroadcastDistributionModel.MODEL_NAME,
								emptyDV,
								emptyIndexes,
								"SELECT b.`id` AS bi0_4",
								"FROM `B` AS b, `C` AS c",
								"WHERE b.`flags` = c.`flags` AND c.`id` > 0"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
								group, "temp2", group, BroadcastDistributionModel.MODEL_NAME,
								emptyDV,
								emptyIndexes,
								"SELECT c.`id` AS ci0_4",
								"FROM `C` AS c, `B` AS b",
								"WHERE c.`flags` = b.`flags` AND b.`id` > 0"
						),
						new UpdateExpectedStep(
								group,
								"UPDATE `D`, temp1, temp2",
								"SET `D`.`desc` = 'foo'",
								"WHERE `D`.`id` = temp1.bi0_4 AND `D`.`id` = temp2.ci0_4"
						)
				));
	}

}
