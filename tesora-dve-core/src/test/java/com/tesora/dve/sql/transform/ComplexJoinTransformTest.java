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
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class ComplexJoinTransformTest extends TransformTest {

	public ComplexJoinTransformTest() {
		super("ComplexJoinTransformTest");
	}
	
	@Test
	public void testSimpleA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A (id int, fid int, primary key (id)) range distribute on (id) using openrange",
				"create table B (id int, fid int, primary key (id)) range distribute on (fid) using openrange",
				"create table C (id int, fid int, primary key (id)) random distribute");
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select a.fid, b.id, c.id + c.fid from A a inner join B b on a.id = b.id - 1 inner join C c on (a.fid + b.fid) = c.id where b.id % 2 = 0",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								bpes(
									bes(
										new ProjectingExpectedStep(ExecutionType.SELECT,
										"SELECT b.`id` AS bi0_4,b.`id` - 1 AS func_5,b.`fid` AS bf1_6 FROM `B` AS b WHERE b.`id` % 2 = 0",
											group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"func_5" }),
										new ProjectingExpectedStep(ExecutionType.SELECT,
										"SELECT DISTINCT temp1.bi0_4 AS t4b0,temp1.func_5 AS t4f1 FROM temp1",
											TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
										new ProjectingExpectedStep(ExecutionType.SELECT,
										"SELECT a.`fid` AS af1_7,a.`id` AS ai0_8 FROM `A` AS a, temp2 WHERE a.`id` = temp2.t4b0 - 1",
											group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_8" })
									)
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT temp3.af1_7 AS t6a0,temp1.bi0_4 AS t7b0,temp1.bf1_6 AS t7b2, (temp3.af1_7 + temp1.bf1_6)  AS func_12 FROM temp3 INNER JOIN temp1 ON temp3.ai0_8 = temp1.func_5",
									TransientExecutionEngine.LARGE,"temp4",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"func_12" }),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT DISTINCT temp4.t6a0 AS t8t0,temp4.t7b2 AS t8t2,temp4.func_12 AS t8f3 FROM temp4",
									TransientExecutionEngine.LARGE,"temp5",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT c.`id` AS ci0_8,c.`fid` AS cf1_9 FROM `C` AS c, temp5 WHERE  (temp5.t8t0 + temp5.t8t2)  = c.`id`",
									group,"temp6",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ci0_8" })
							)
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT temp4.t6a0 AS t10t0_13,temp4.t7b0 AS t10t1_14,temp6.ci0_8 + temp6.cf1_9 AS func FROM temp4 INNER JOIN temp6 ON temp4.func_12 = temp6.ci0_8",
							null)
					));
	}

	@Test
	public void testSimpleB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A (id int, fid int, primary key (id)) range distribute on (id) using openrange",
				"create table B (id int, fid int, primary key (id)) range distribute on (fid) using openrange",
				"create table C (id int, fid int, primary key (id)) random distribute");
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select a.fid, b.id, c.id + c.fid from A a inner join B b on a.id = b.id - 1 inner join C c on c.id = (a.fid + b.fid) where b.id % 2 = 0",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								bpes(
									bes(
										new ProjectingExpectedStep(ExecutionType.SELECT,
										"SELECT b.`id` AS bi0_4,b.`id` - 1 AS func_5,b.`fid` AS bf1_6 FROM `B` AS b WHERE b.`id` % 2 = 0",
											group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"func_5" }),
										new ProjectingExpectedStep(ExecutionType.SELECT,
										"SELECT DISTINCT temp1.bi0_4 AS t4b0,temp1.func_5 AS t4f1 FROM temp1",
											TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
										new ProjectingExpectedStep(ExecutionType.SELECT,
										"SELECT a.`fid` AS af1_7,a.`id` AS ai0_8 FROM `A` AS a, temp2 WHERE a.`id` = temp2.t4b0 - 1",
											group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_8" })
									)
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT temp3.af1_7 AS t6a0,temp1.bi0_4 AS t7b0,temp1.bf1_6 AS t7b2, (temp3.af1_7 + temp1.bf1_6)  AS func_12 FROM temp3 INNER JOIN temp1 ON temp3.ai0_8 = temp1.func_5",
									TransientExecutionEngine.LARGE,"temp4",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"func_12" }),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT DISTINCT temp4.t6a0 AS t8t0,temp4.t7b2 AS t8t2,temp4.func_12 AS t8f3 FROM temp4",
									TransientExecutionEngine.LARGE,"temp5",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT c.`id` AS ci0_8,c.`fid` AS cf1_9 FROM `C` AS c, temp5 WHERE c.`id` =  (temp5.t8t0 + temp5.t8t2)",
									group,"temp6",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ci0_8" })
							)
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT temp4.t6a0 AS t10t0_13,temp4.t7b0 AS t10t1_14,temp6.ci0_8 + temp6.cf1_9 AS func FROM temp4 INNER JOIN temp6 ON temp6.ci0_8 = temp4.func_12",
							null)
					));
	}

	@Test
	public void testSimpleC() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A (id int, fid int) range distribute on (id) using openrange",
				"create table B (id int, fid int) range distribute on (fid) using openrange");
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select a.fid, b.id from A a inner join B b on a.id = b.id - 1",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT a.`fid` AS af1_3,a.`id` AS ai0_4 FROM `A` AS a",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_4" })
							),
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT b.`id` AS bi0_3,b.`id` - 1 AS func_4 FROM `B` AS b",
									group,"temp2",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"func_4" })
							)
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT temp1.af1_3 AS t3a0_10,temp2.bi0_3 AS t4b0_11 FROM temp1 INNER JOIN temp2 ON temp1.ai0_4 = temp2.func_4",
							null)
					));
	}
	
}
