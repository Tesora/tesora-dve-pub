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

import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class TriggerTransformTest extends TransformTest {

	public TriggerTransformTest() {
		super("TriggerTransformTest");
	}
	
	@Test
	public void testA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create range arange (int) persistent group g1",
				"create range brange (int) persistent group g1",
				"create table ref (id int, subject varchar(32), primary key (id)) range distribute on (id) using arange",
				"create table lookup (id int, kind int, targ int, primary key (id)) range distribute on (id) using brange",
				"create table subj (id int, action varchar(32), primary key (id)) range distribute on (id) using arange",
				"create trigger subj_upd after update on subj for each row "
				+"begin update ref r inner join lookup l on r.id = l.targ set r.subject = NEW.action where l.kind = 5 and OLD.action like '%whatevs%'; END");
	
		String sql =
				"update subj s inner join lookup l on s.id = l.targ  set s.action = 'firsttest' where l.kind = 5";
		
		PEStorageGroup group = getGroup(db);
		stmtTest(db,sql,UpdateStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp5",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
							new String[] {"lt2_2" },
							new String[][] {{"lt2_2"} },
						  "SELECT l.`targ` AS lt2_2",
						  "FROM `lookup` AS l",
						  "WHERE l.`kind` = 5"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp6",group,BroadcastDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"t8l0"} },
						  "SELECT DISTINCT temp5.lt2_2 AS t8l0",
						  "FROM temp5"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp7",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
							new String[] {"si0_7" },
							new String[][] {{"si0_7"} },
						  "SELECT s.`action` AS sa1_6,s.`id` AS si0_7",
						  "FROM `subj` AS s, temp6",
						  "WHERE s.`id` = temp6.t8l0"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp8",TransientExecutionEngine.AGGREGATION,BroadcastDistributionModel.MODEL_NAME,
							emptyDV,
							emptyIndexes,
						  "SELECT temp7.sa1_6 AS t10s0_7,'firsttest' AS litex,temp7.si0_7 AS t10s1_9",
						  "FROM temp7",
						  "INNER JOIN temp5 ON temp7.si0_7 = temp5.lt2_2"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.TRIGGER_SRC_TABLE)),
						new TriggerExpectedStep(group,
						  "SELECT temp8.t10s0_7,temp8.litex,temp8.t10s1_9 FROM temp8",
							bes(
								new UpdateExpectedStep(
									group,
								  "UPDATE `subj` AS s",
								  "SET s.`action` = _lbc1",
								  "WHERE s.`id` = _lbc2"
								)
							),
						  null,
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
									new String[] {"lt2_2" },
									new String[][] {{"lt2_2"} },
								  "SELECT l.`targ` AS lt2_2",
								  "FROM `lookup` AS l",
								  "WHERE l.`kind` = 5"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT)),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,
									emptyDV,
									new String[][] {{"t3l0"} },
								  "SELECT DISTINCT temp1.lt2_2 AS t3l0",
								  "FROM temp1"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE)),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
									new String[] {"ri0_4" },
									new String[][] {{"ri0_4"} },
								  "SELECT r.`id` AS ri0_4",
								  "FROM `ref` AS r, temp2",
								  "WHERE r.`id` = temp2.t3l0"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN)),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.LARGE,"temp4",group,BroadcastDistributionModel.MODEL_NAME,
									emptyDV,
									emptyIndexes,
								  "SELECT temp3.ri0_4 AS t5r0_5",
								  "FROM temp3",
								  "INNER JOIN temp1 ON temp3.ri0_4 = temp1.lt2_2",
								  "WHERE _lbc0 like '%whatevs%'"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT)),
								new UpdateExpectedStep(
									group,
								  "UPDATE `ref` AS r",
								  "INNER JOIN temp4 ON r.`id` = temp4.t5r0_5",
								  "SET r.`subject` = _lbc0"
								)
							)
					)
					));
		
	}
	
	@Test
	public void testB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create range arange (int) persistent group g1",
				"create range brange (int) persistent group g1",
				"create table ref (id int, subject varchar(32), primary key (id)) range distribute on (id) using arange",
				"create table lookup (id int, kind int, targ int, primary key (id)) range distribute on (id) using brange",
				"create table subj (id int, action varchar(32), primary key (id)) range distribute on (id) using arange",
				"create trigger subj_del after delete on subj for each row "
				+"begin update ref r inner join lookup l on r.id = l.targ set r.subject = OLD.action where l.kind = 5 and OLD.id % 2 = 0; END"
				);

		String sql =
				"delete s from subj s inner join lookup l on s.id = l.targ where l.kind = 5";

		PEStorageGroup group = getGroup(db);
		
		stmtTest(db,sql,DeleteStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp5",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
							new String[] {"lt2_2" },
							new String[][] {{"lt2_2"} },
						  "SELECT l.`targ` AS lt2_2",
						  "FROM `lookup` AS l",
						  "WHERE l.`kind` = 5"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp6",group,BroadcastDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"t8l0"} },
						  "SELECT DISTINCT temp5.lt2_2 AS t8l0",
						  "FROM temp5"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp7",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
							new String[] {"si0_7" },
							new String[][] {{"si0_7"} },
						  "SELECT s.`action` AS sa1_6,s.`id` AS si0_7",
						  "FROM `subj` AS s, temp6",
						  "WHERE s.`id` = temp6.t8l0"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp8",TransientExecutionEngine.AGGREGATION,BroadcastDistributionModel.MODEL_NAME,
							emptyDV,
							emptyIndexes,
						  "SELECT temp7.sa1_6 AS t10s0_7,temp7.si0_7 AS t10s1_8",
						  "FROM temp7",
						  "INNER JOIN temp5 ON temp7.si0_7 = temp5.lt2_2"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.TRIGGER_SRC_TABLE)),
						new TriggerExpectedStep(group,
						  "SELECT temp8.t10s0_7,temp8.t10s1_8 FROM temp8",
							bes(
								new DeleteExpectedStep(
									group,
								  "DELETE s",
								  "FROM `subj` AS s",
								  "WHERE s.`id` = _lbc1"
								)
							),
						  null,
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
									new String[] {"lt2_2" },
									new String[][] {{"lt2_2"} },
								  "SELECT l.`targ` AS lt2_2",
								  "FROM `lookup` AS l",
								  "WHERE l.`kind` = 5"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT)),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,
									emptyDV,
									new String[][] {{"t3l0"} },
								  "SELECT DISTINCT temp1.lt2_2 AS t3l0",
								  "FROM temp1"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE)),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,
									new String[] {"ri0_4" },
									new String[][] {{"ri0_4"} },
								  "SELECT r.`id` AS ri0_4",
								  "FROM `ref` AS r, temp2",
								  "WHERE r.`id` = temp2.t3l0"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN)),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.LARGE,"temp4",group,BroadcastDistributionModel.MODEL_NAME,
									emptyDV,
									emptyIndexes,
								  "SELECT temp3.ri0_4 AS t5r0_5",
								  "FROM temp3",
								  "INNER JOIN temp1 ON temp3.ri0_4 = temp1.lt2_2",
								  "WHERE _lbc1 % 2 = 0"
								)
								.withExplain(new DMLExplainRecord(DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT)),
								new UpdateExpectedStep(
									group,
								  "UPDATE `ref` AS r",
								  "INNER JOIN temp4 ON r.`id` = temp4.t5r0_5",
								  "SET r.`subject` = _lbc0"
								)
							)
					)
					));


		
	}
	
}
