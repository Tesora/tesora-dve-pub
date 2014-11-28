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

import org.junit.Assert;
import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class AggregationTransformTest extends TransformTest {

	public AggregationTransformTest() {
		super("AggregationTransformTest");
	}
	
	private static final String[] leftySchema = new String[] {
		"create table `titles` (`id` int unsigned not null, `name` varchar(50) not null) static distribute on (`id`)",
		"create table `states` (`id` int unsigned not null, `name` varchar(50) not null, `tag` varchar(50)) static distribute on (`id`)",
		"create table `laws` (`id` int unsigned not null, `state_id` int unsigned not null, `title_id` int unsigned not null, " 
			+ "`status` varchar(16) not null default 'unpublished', `version` int unsigned not null, `law` varchar(100)) "
			+ "static distribute on (`id`)",
		"create table `counties` (`id` int unsigned not null, `name` varchar(50) not null, `state_id` int unsigned not null) " +
			"static distribute on (`id`)",
		"create table `courts` (`id` int unsigned not null, `county_id` int unsigned not null, `address` varchar(50) not null) " +
			"static distribute on (`id`)"
	};
	
	private static final String[] aggTestSchema = new String[] {
		"create table `B` ( `id` int, `sid` int, `pa` int(10), `pb` int(10), primary key (`id`) ) broadcast distribute",
		"create table `S` ( `id` int, `sid` int, `pa` int(10), `pb` int(10), primary key (`id`) ) static distribute on (`id`)",
		"create table `A` ( `id` int, `sid` int, `pa` int(10), `pb` int(10), primary key (`id`) ) random distribute",
		"create table `R` ( `id` int, `sid` int, `pa` int(10), `pb` int(10), primary key (`id`) ) range distribute on (`id`) using openrange",
	};
	
	@Test
	public void simpleMultiMaxTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select max(version) from laws",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT max( `laws`.`version` )  AS func_4 FROM `laws`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT max( temp1.func_4 )  AS func FROM temp1",
								null)));
	}
	
	
	@Test
	public void simpleMultiMinTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select min ( version ) from laws",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT min( `laws`.`version` )  AS func_4 FROM `laws`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT min( temp1.func_4 )  AS func FROM temp1",
								null)));
		
	}
	
	
	@Test
	public void simpleMultiCountTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(*) from laws",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT count( * )  AS func_4 FROM `laws`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT CONVERT( sum( temp1.func_4 ) ,SIGNED )  AS func FROM temp1",
								null)));
		
	}

	
	@Test
	public void simpleMultiSumTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select sum(version) from laws",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT sum( `laws`.`version` )  AS func_4 FROM `laws`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT sum( temp1.func_4 )  AS func FROM temp1",
								null)));
		
	}

	
	@Test
	public void simpleMultiAvgTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select avg(version) from laws",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT sum( `laws`.`version` )  AS func_4,COUNT( `laws`.`version` )  AS func_5 FROM `laws`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT  (sum( temp1.func_4 )  / sum( temp1.func_5 ) )  AS func FROM temp1",
								null)
				));
	}

	
	@Test
	public void simpleSingleSumTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		stmtTest(db,
				"select sum(version) from laws where id = 15",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT sum( `laws`.`version` )  AS func_5 FROM `laws` WHERE `laws`.`id` = 15",
								null )));
	}

	
	@Test
	public void simpleSingleCountTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		stmtTest(db,
				"select count(*) from laws where id = 15",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT count( * )  AS func_5 FROM `laws` WHERE `laws`.`id` = 15",
								null )));
	}

	
	@Test
	public void simpleSingleMaxTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		stmtTest(db,
				"select max(version) from laws where id = 15",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT max( `laws`.`version` )  AS func_5 FROM `laws` WHERE `laws`.`id` = 15",
								null )));
	}

	
	
	@Test
	public void simpleSingleAvgTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		stmtTest(db,
				"select avg(version) from laws where id = 15",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT  (sum( `laws`.`version` )  / COUNT( `laws`.`version` ) )  AS func_5 FROM `laws` WHERE `laws`.`id` = 15",
								null)));
	}
	
	
	@Test 
	public void testPE396A() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select l.state_id + l.title_id + max(l.version) from laws l",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT l.state_id + l.title_id AS func_6,max( l.version )  AS func_7 FROM `laws` AS l",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT temp1.func_6 + max( temp1.func_7 )  AS func FROM temp1",
								null)));
	}
	
	
	@Test
	public void testComplexMultiTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select min(version), max(version), avg(version), min(version) + max(version) from laws l",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT min( l.`version` )  AS func_7,max( l.`version` )  AS func_8,sum( l.`version` )  AS func_9,COUNT( l.`version` )  AS func_10,min( l.`version` )  AS func_11,max( l.`version` )  AS func_12",
						  "FROM `laws` AS l"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT min( temp1.func_7 )  AS func,max( temp1.func_8 )  AS func_14, (sum( temp1.func_9 )  / sum( temp1.func_10 ) )  AS func_15,min( temp1.func_11 )  + max( temp1.func_12 )  AS func_16",
						  "FROM temp1"
						)
					)
				);
	}

	
	@Test
	public void testRedistA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(*) from (select 1 as expression from laws where version > 1) subquery",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT count( * )  AS func_5 FROM `laws` WHERE `laws`.`version` > 1",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT CONVERT( sum( temp1.func_5 ) ,SIGNED )  AS func FROM temp1",
								null)));
	}
	
	@Test
	public void testPE396B() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select avg(version) + state_id, law from laws l group by law",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT sum( l.`version` )  AS func_8,COUNT( l.`version` )  AS func_9,l.`state_id` AS ls1_10,l.`law` AS ll5_11 FROM `laws` AS l GROUP BY ll5_11 ASC",
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ll5_11" }),
						new ProjectingExpectedStep(
						"SELECT  (sum( temp1.func_8 )  / sum( temp1.func_9 ) )  + temp1.ls1_10 AS func,temp1.ll5_11 AS t2l3_10 FROM temp1 GROUP BY t2l3_10 ASC",
							null)
					));
	}

	@Test
	public void testPE396C() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		stmtTest(db,
				"select avg(version) + state_id, law from laws l group by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT  (sum( l.`version` )  / COUNT( l.`version` ) )  + l.`state_id` AS func_11,l.`law` AS ll5_12 FROM `laws` AS l GROUP BY l.`id` ASC",
								null)));
	}

	// take the new plan here too
	@Test
	public void testAggTestNonGrandAggFunsE() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,aggTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(pb) + 15, sid, pa from R group by pa",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"R1p2_11" }, new String[][] {{"R1p2_11"} },
						  "SELECT count( `R`.`pb` )  AS func_9,`R`.`sid` AS R1s1_10,`R`.`pa` AS R1p2_11",
						  "FROM `R`",
						  "GROUP BY R1p2_11 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT CONVERT( sum( temp1.func_9 ) ,SIGNED )  + 15 AS func,temp1.R1s1_10 AS t2R1_8,temp1.R1p2_11 AS t2R2_9",
						  "FROM temp1",
						  "GROUP BY t2R2_9 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.func AS t3f0,temp2.t2R1_8 AS t3t1,temp2.t2R2_9 AS t3t2",
						  "FROM temp2",
						  "ORDER BY t3t2 ASC"
						)
					)
					);
	}

	@Test
	public void testAggTestDistinctGrandAggA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,aggTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(distinct id) from A",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"A1i0_4" }, new String[][] {{"A1i0_4"} },
						  "SELECT DISTINCT `A`.`id` AS A1i0_4",
						  "FROM `A`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT count( DISTINCT temp1.A1i0_4 )  AS func",
						  "FROM temp1"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT CONVERT( sum( temp2.func ) ,SIGNED )  AS func_3",
						  "FROM temp2"
						)
					)
					);
	}

	@Test
	public void testAggTestGrandAggA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,aggTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(*) from A",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT count( * )  AS func_4 FROM `A`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] {}),
						new ProjectingExpectedStep(
								"SELECT CONVERT( sum( temp1.func_4 ) ,SIGNED )  AS func FROM temp1",
								null)));
	}

	@Test
	public void testAggTestEntityApi45355() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,aggTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select distinct s.pa, a.pb from S s inner join A a on s.sid = a.sid where a.pb > 5 and s.pb > 5",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT s.`pa` AS sp2_3,s.`sid` AS ss1_4 FROM `S` AS s WHERE s.`pb` > 5",
									group,"temp1",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,new String[] {"ss1_4" })
							),
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`pb` AS ap3_3,a.`sid` AS as1_4 FROM `A` AS a WHERE a.`pb` > 5",
									group,"temp2",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,new String[] {"as1_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.sp2_3 AS t3s0_9,temp2.ap3_3 AS t4a0_10 FROM temp1 INNER JOIN temp2 ON temp1.ss1_4 = temp2.as1_4",
							TransientExecutionEngine.MEDIUM,"temp3",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,new String[] {"t3s0_9","t4a0_10" }),
						new ProjectingExpectedStep(
						"SELECT DISTINCT temp3.t3s0_9 AS t5t0,temp3.t4a0_10 AS t5t1 FROM temp3 GROUP BY t5t0 ASC, t5t1 ASC",
							null)
					));
	}
	
	@Test
	public void testHavingA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,aggTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select max(pa), sid from A a group by sid having avg(pb) > 4",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"as1_11" }, new String[][] {{"as1_11"} },
						  "SELECT max( a.`pa` )  AS func_10,a.`sid` AS as1_11,sum( a.`pb` )  AS func_12,COUNT( a.`pb` )  AS func_13",
						  "FROM `A` AS a",
						  "GROUP BY as1_11 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT max( temp1.func_10 )  AS func,temp1.as1_11 AS t2a1_10",
						  "FROM temp1",
						  "GROUP BY t2a1_10 ASC",
						  "HAVING  (sum( temp1.func_12 )  / sum( temp1.func_13 ) )  > 4"
						)
					)
					);
	}
	
	@Test
	public void testHavingB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,aggTestSchema);		
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select max(pa), sid, avg(pb) hc from A a group by sid having hc > 6",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"as1_12" }, new String[][] {{"as1_12"} },
						  "SELECT max( a.`pa` )  AS func_11,a.`sid` AS as1_12,sum( a.`pb` )  AS func_13,COUNT( a.`pb` )  AS func_14",
						  "FROM `A` AS a",
						  "GROUP BY as1_12 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT max( temp1.func_11 )  AS func,temp1.as1_12 AS t2a1_10, (sum( temp1.func_13 )  / sum( temp1.func_14 ) )  AS func_12",
						  "FROM temp1",
						  "GROUP BY t2a1_10 ASC",
						  "HAVING func_12 > 6"
						)
					)
					);
	}

	@Test
	public void testHavingC() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,aggTestSchema);		
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"Select pa, pb, sid from A a group by sid having pa < 0",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,new String[] {"as1_13" }, new String[][] {{"as1_13"} },
						  "SELECT a.`pa` AS ap2_11,a.`pb` AS ap3_12,a.`sid` AS as1_13",
						  "FROM `A` AS a",
						  "WHERE a.`pa` < 0"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.MEDIUM,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.ap2_11 AS t2a0,temp1.ap3_12 AS t2a1,temp1.as1_13 AS t2a2",
						  "FROM temp1",
						  "GROUP BY t2a2 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.t2a0 AS t3t0,temp2.t2a1 AS t3t1,temp2.t2a2 AS t3t2",
						  "FROM temp2",
						  "ORDER BY t3t2 ASC"
						)
					)
					);
	}

	@Test
	public void testPE169Random() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table sbtest (id int, primary key (id) ) random distribute");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(distinct id) from sbtest",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT DISTINCT `sbtest`.`id` AS s1i0_4 FROM `sbtest`",
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"s1i0_4" }),
						new ProjectingExpectedStep(
						"SELECT count( DISTINCT temp1.s1i0_4 )  AS func FROM temp1",
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT CONVERT( sum( temp2.func ) ,SIGNED )  AS func_3 FROM temp2",
							null)
					));
	}
	
	@Test
	public void testPE169BCast() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
			"create table sbtest (id int, primary key (id) ) broadcast distribute");
		stmtTest(db,
				"select count(distinct id) from sbtest",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT count( DISTINCT `sbtest`.`id` )  AS func FROM `sbtest`",
							null)
					));
	}
	
	@Test
	public void testPE169Static() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
		"create table sbtest (id int, primary key (id) ) static distribute on (`id`)");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(distinct id) from sbtest",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT count( DISTINCT `sbtest`.`id` )  AS func_4",
						  "FROM `sbtest`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT CONVERT( sum( temp1.func_4 ) ,SIGNED )  AS func",
						  "FROM temp1"
						)
					));
	}
	
	@Test
	public void testPE169Range() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
			"create table sbtest (id int, primary key (id) ) range distribute on (`id`) using openrange");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(distinct id) from sbtest",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT count( DISTINCT `sbtest`.`id` )  AS func_4 FROM `sbtest`",
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT CONVERT( sum( temp1.func_4 ) ,SIGNED )  AS func FROM temp1",
							null)
					));
	}
	
	@Test
	public void testPE287() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A ("
				+ "fname varchar(256), caller varchar(256), callee varchar(256), "
				+ "ct int, wt int, cputime int, mu int, pmu int) random distribute ");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		String sql = "select fname, caller, max(wt/ct) from A where caller = 'main()'";
		stmtTest(db,
				sql,
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT `A`.`fname` AS A1f0_10,`A`.`caller` AS A1c1_11 FROM `A` WHERE `A`.`caller` = 'main()' LIMIT 1",
							group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp2.A1f0_10 AS t3A0,temp2.A1c1_11 AS t3A1 FROM temp2 LIMIT 1",
							TransientExecutionEngine.AGGREGATION,"temp3",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT max( `A`.`wt` / `A`.`ct` )  AS func_10 FROM `A` WHERE `A`.`caller` = 'main()'",
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp3.t3A0 AS t4t0,temp3.t3A1 AS t4t1,max( temp1.func_10 )  AS func_6 FROM temp1, temp3",
							null)
					));
	}

	@Test
	public void testPE396D() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A ("
				+ "fname varchar(256), caller varchar(256), callee varchar(256), "
				+ "ct int, wt int, cputime int, mu int, pmu int) random distribute ");
//		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		String sql = "select max(wt/ct) + cputime, caller from A where caller = 'main()'";
		stmtTest(db,
				sql,
				SelectStatement.class,
				null);			
	}

	@Test
	public void testResolutionA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
					"create table foo (id int, pid int, sid int) random distribute");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		// notice that the order by is on foo.id, so it should be put into the projection
		stmtTest(db,
				"select max(id) as id, pid as pid, sid as sid from foo group by pid, sid order by foo.id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"f1p1_13","f1s2_14" }, 
							new String[][] {{"f1p1_13","f1s2_14"} },
						  "SELECT max( `foo`.`id` )  AS func,`foo`.`pid` AS f1p1_13,`foo`.`sid` AS f1s2_14,`foo`.`id` AS f1i0_15",
						  "FROM `foo`",
						  "GROUP BY f1p1_13 ASC, f1s2_14 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT max( temp1.func )  AS func_9,temp1.f1p1_13 AS t2f1_10,temp1.f1s2_14 AS t2f2_11,temp1.f1i0_15 AS t2f3_12",
						  "FROM temp1",
						  "GROUP BY t2f1_10 ASC, t2f2_11 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.func_9 AS t3f0,temp2.t2f1_10 AS t3t1,temp2.t2f2_11 AS t3t2",
						  "FROM temp2",
						  "ORDER BY temp2.t2f3_12 ASC"
						)
					)
					);
	}

	@Test
	public void testResolutionB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
					"create table foo (id int, pid int, sid int) random distribute");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		// notice that the order by is on id - that is, the max(id) column
		stmtTest(db,
				"select max(id) as id, pid as pid, sid as sid from foo group by pid, sid order by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"f1p1_9","f1s2_10" }, 
							new String[][] {{"f1p1_9","f1s2_10" } },
						  "SELECT max( `foo`.`id` )  AS func,`foo`.`pid` AS f1p1_9,`foo`.`sid` AS f1s2_10",
						  "FROM `foo`",
						  "GROUP BY f1p1_9 ASC, f1s2_10 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT max( temp1.func )  AS func_7,temp1.f1p1_9 AS t2f1_8,temp1.f1s2_10 AS t2f2_9",
						  "FROM temp1",
						  "GROUP BY t2f1_8 ASC, t2f2_9 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.func_7 AS t3f0,temp2.t2f1_8 AS t3t1,temp2.t2f2_9 AS t3t2",
						  "FROM temp2",
						  "ORDER BY t3f0 ASC"
						)
					)
					);
	}
	
	@Test
	public void testPE457() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table foo (`id` int, `sid` int, `pid` int, primary key (`id`)) static distribute on (`id`)");

//		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(*) from foo where id in (3,4)",
				SelectStatement.class,
				null);
	}
	
	@Test
	public void testPE288() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		String[] unsupported = new String[] { "bit_and", "bit_or", "bit_xor", "std", "stddev_pop",
				"stddev_samp", "stddev", "var_pop", "var_samp", "variance" };
		for(String f : unsupported) {
			try {
				stmtTest(db,
						"select " + f + "(version) from laws group by id",
						SelectStatement.class,
						null);
				Assert.fail("Did not fail on unsupported agg fun " + f);
			} catch (Throwable t) {
				SchemaTest.assertSchemaException(t, "Unsupported aggregation function: " + f);
			}
		}
	}
	
	@Test
	public void testPE702() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table t1 (`id` int, `fid` int, `sid` int, primary key (`id`)) random distribute");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select count(distinct id, fid, sid) from t1",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT `t1`.`id` AS t1i0_6,`t1`.`fid` AS t1f1_7,`t1`.`sid` AS t1s2_8",
						  "FROM `t1`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT count( DISTINCT temp1.t1i0_6,temp1.t1f1_7,temp1.t1s2_8 )  AS func",
						  "FROM temp1"
						)
					)
					);
	}

	@Test
	public void testPE797A() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, aggTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select a.sid, count(a.sid), count(distinct a.sid) from A a group by a.sid",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT a.`sid` AS as1_8 FROM `A` AS a",
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"as1_8" }),
						new ProjectingExpectedStep(
						"SELECT temp1.as1_8 AS t2a0_3,count( temp1.as1_8 )  AS func,count( DISTINCT temp1.as1_8 )  AS func_5 FROM temp1 GROUP BY t2a0_3 ASC",
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT temp2.t2a0_3 AS t3t0,temp2.func AS t3f1,temp2.func_5 AS t3f2 FROM temp2 ORDER BY t3t0 ASC",
							null)
					));
	}

	@Test
	public void testPE797B() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, aggTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select a.sid, avg(distinct a.pa) from S a group by a.sid",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"as1_4" }, new String[][] {{"as1_4"} },
						  "SELECT a.`sid` AS as1_4,a.`pa` AS ap2_6",
						  "FROM `S` AS a"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp1.as1_4 AS t2a0_5, (sum( DISTINCT temp1.ap2_6 )  / COUNT( DISTINCT temp1.ap2_6 ) )  AS func",
						  "FROM temp1",
						  "GROUP BY t2a0_5 ASC"
						)
					)
					);
	}
       
	@Test
	public void testPE865() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe865 (id int auto_increment, n int, s varchar(32), fp int, primary key(id)) static distribute on (id)");
		stmtTest(db,
				"select fp, group_concat(s separator ' ') from pe865 group by fp",
				SelectStatement.class,
				null);
	}
	
	private static final String bigAggTabBody = 
			" `id` int auto_increment, `e` int, `d` int, `c` int, `b` int, `a` int, primary key (id) ";

	
	private static final String[] bigAggSchema = new String[] {
		"create table A (" + bigAggTabBody + ") random distribute",
		"create table B (" + bigAggTabBody + ") broadcast distribute",
		"create table R (" + bigAggTabBody + ") range distribute on (id) using openrange",
		"create table S (" + bigAggTabBody + ") static distribute on (id)"
	};
	
	@Test
	public void testBigAggAS() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, bigAggSchema);
		stmtTest(db, "select count(distinct e), count(distinct d), count(distinct c), count(distinct b), count(distinct a) from S",
				SelectStatement.class,null);
	}
	
	@Test
	public void testBigAggDA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, bigAggSchema);
		stmtTest(db, "select count(e), count(distinct e), e from A group by e",
				SelectStatement.class,
				null);
	}

	@Test
	public void testPE1309() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"CREATE TABLE `keyword_url_facts` (`id` int(10) unsigned NOT NULL,`time_period_id` int(10) unsigned NOT NULL,`domain_id` int(10) unsigned NOT NULL,`url_id` int(10) unsigned NOT NULL,`keyword_id` int(10) unsigned NOT NULL,`keyword_url_id` int(10) unsigned NOT NULL,`conductor_score` tinyint(3) unsigned DEFAULT NULL,`google_search_rank` tinyint(3) unsigned DEFAULT NULL,`meta_desc_present` bit(1) DEFAULT NULL,`google_page_rank` tinyint(3) unsigned DEFAULT NULL,`backlinks` int(10) unsigned DEFAULT NULL,`prime` bit(1) DEFAULT NULL,`url_outlinks_count` int(10) unsigned DEFAULT NULL,`body_match_count` int(10) unsigned DEFAULT NULL,`title_partial_match_count` smallint(5) unsigned DEFAULT NULL,`meta_desc_partial_match_count` int(10) unsigned DEFAULT NULL,`title_first_match_position` smallint(5) unsigned DEFAULT NULL,`body_first_match_position` int(10) unsigned DEFAULT NULL,`meta_first_match_position` smallint(5) unsigned DEFAULT NULL,`title_length` int(10) unsigned DEFAULT NULL,`meta_desc_length` smallint(5) unsigned DEFAULT NULL,`outlink_nofol_ext_count` int(10) unsigned DEFAULT NULL,`outlink_fol_ext_count` int(10) unsigned DEFAULT NULL,`outlink_nofol_int_count` int(10) unsigned DEFAULT NULL,`outlink_fol_int_count` int(10) unsigned DEFAULT NULL,`ave_download_time` int(10) unsigned DEFAULT NULL,`page_title` bit(1) NOT NULL,`local_google_monthly_search_volume` bigint(20) unsigned DEFAULT NULL,`global_google_monthly_search_volume` bigint(20) unsigned DEFAULT NULL,`bing_search_rank` tinyint(3) unsigned DEFAULT NULL,`omniture_visits` bigint(20) unsigned DEFAULT NULL,`omniture_google_referrals` bigint(20) unsigned DEFAULT NULL,`recommendation_health` int(10) DEFAULT NULL,PRIMARY KEY (`id`),UNIQUE KEY `uq_keyword_url_facts_1` (`time_period_id`, `keyword_url_id`),UNIQUE KEY `uk_keyword_url_facts_2` (`time_period_id`, `keyword_id`, `url_id`),KEY `idx_keyword_url_facts_3` (`url_id`),KEY `idx_keyword_url_facts_5` (`keyword_url_id`),KEY `domain_id` (`domain_id`, `time_period_id`),KEY `keyword_id` (`keyword_id`, `time_period_id`, `domain_id`, `google_search_rank`)) ENGINE=InnoDB DEFAULT CHARSET=utf8  RANDOM DISTRIBUTE",
				"CREATE TABLE `keyword_url_relationship` (`id` bigint(20) unsigned NOT NULL,`time_period_id` int(10) unsigned NOT NULL,`keyword_id` int(10) unsigned NOT NULL,`url_id` int(10) unsigned NOT NULL,`keyword_url_relationship_type_id` tinyint(3) unsigned NOT NULL,PRIMARY KEY (`id`),UNIQUE KEY `uk_keyword_url_relationship` (`keyword_id`, `url_id`, `time_period_id`, `keyword_url_relationship_type_id`),KEY `idx_time_period` (`time_period_id`, `keyword_id`, `url_id`),KEY `idx_url` (`url_id`, `keyword_id`, `time_period_id`),KEY `idx_keyword_url_relationship_type_id` (`keyword_url_relationship_type_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8  RANDOM DISTRIBUTE",
				"CREATE TABLE `urls` (`id` int(10) unsigned NOT NULL,`domain_id` int(10) unsigned NOT NULL,`url` varchar(1000) NOT NULL,`created` datetime(19) NOT NULL,`modified` datetime(19) NOT NULL,`url_hash` varbinary(32) NOT NULL,`link_metric_id` int(10) unsigned DEFAULT NULL,`config_url_id` int(10) unsigned DEFAULT NULL,PRIMARY KEY (`id`),KEY `idx_urls_1` (`domain_id`),KEY `idx_url_hash` (`url_hash`),KEY `idx_urls_2` (`link_metric_id`),KEY `config_url_id` (`config_url_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8  RANDOM DISTRIBUTE",
				"CREATE TABLE `keyword_url_recommendations` (`id` int(10) NOT NULL,`keyword_url_id` int(10) unsigned NOT NULL,`issue_type_id` int(10) NOT NULL,`time_period_id` int(10) DEFAULT NULL,PRIMARY KEY (`id`),KEY `issue_type_id` (`issue_type_id`),KEY `idx_kur_tp_kurl` (`time_period_id`, `keyword_url_id`),KEY `keyword_url_id` (`keyword_url_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8  RANDOM DISTRIBUTE",
				"CREATE TABLE `keywords` (`id` int(10) unsigned NOT NULL,`domain_id` int(10) unsigned NOT NULL,`keyword` varchar(255) NOT NULL,`created` datetime(19) NOT NULL,`modified` datetime(19) NOT NULL,`prime` bit(1) NOT NULL,`raw_keyword` varchar(255) DEFAULT NULL,PRIMARY KEY (`id`),UNIQUE KEY `domain_id_2` (`domain_id`, `keyword`),KEY `domain_id` (`domain_id`, `prime`),KEY `keyword` (`keyword`),KEY `raw_keyword` (`raw_keyword`)) ENGINE=InnoDB DEFAULT CHARSET=utf8  RANDOM DISTRIBUTE"
				);
		String sql =
				"SELECT keywordurl0_.keyword_id AS col_0_0_,url2_.url AS col_1_0_,keywordurl0_.url_id AS col_2_0_, keywordurl0_.google_search_rank AS col_3_0_, "
				+"(SELECT count( * )  "
				+" FROM `keyword_url_facts` AS keywordurl3_ "
				+" INNER JOIN `keyword_url_recommendations` AS keywordurl4_ "
				+"   ON keywordurl3_.keyword_url_id = keywordurl4_.keyword_url_id "
				+"   and keywordurl3_.time_period_id = keywordurl4_.time_period_id "
				+" WHERE keywordurl3_.id = keywordurl0_.id) AS col_4_0_,"
				+"coalesce( (SELECT count( * )  "
				+"           FROM `keyword_url_relationship` AS keywordurl5_, `keywords` AS keyword6_, `urls` AS url8_ "
				+"           INNER JOIN `keywords` AS keyword7_ "
				+"           WHERE keywordurl5_.keyword_id = keyword6_.id and keywordurl5_.url_id = url8_.id "
				+"             and keywordurl0_.keyword_id = keyword7_.id and keywordurl5_.keyword_url_relationship_type_id = 2 "
				+"             and keywordurl5_.time_period_id = 202 and keywordurl5_.keyword_id = keywordurl0_.keyword_id "
				+"             and keywordurl5_.url_id = keywordurl0_.url_id),0 )  AS col_5_0_ "
				+"FROM `keyword_url_facts` AS keywordurl0_ "
				+"INNER JOIN `keyword_url_relationship` AS keywordurl1_ "
				+"  ON keywordurl0_.keyword_id = keywordurl1_.keyword_id and keywordurl0_.url_id = keywordurl1_.url_id "
				+"  and keywordurl0_.time_period_id = keywordurl1_.time_period_id, `urls` AS url2_ "
				+"WHERE keywordurl0_.url_id = url2_.id and  (keywordurl0_.keyword_id BETWEEN 188606 AND 1839491)  "
				+"and  (keywordurl0_.keyword_id in ( 1839273))  and keywordurl0_.domain_id = 413205 and keywordurl0_.time_period_id = 202 "
				+"GROUP BY col_2_0_ ASC, col_0_0_ ASC ORDER BY col_3_0_ ASC LIMIT 2147483647";
		stmtTest(db,sql,SelectStatement.class,
				null);
		
	}

	@Test
	public void testPE1350() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"CREATE TABLE `cache_exec` (`id_cache_exec` int(10) NOT NULL AUTO_INCREMENT,`id_ref_cache_exec_status` int(10) NOT NULL DEFAULT '1',"
				+"`id_ref_cache_exec_type` int(10) NOT NULL DEFAULT '1',`id_ref_cache_type` int(10) NOT NULL DEFAULT '1',`start_time` int(10) NOT NULL DEFAULT '0',"
				+"`end_time` int(10) NOT NULL DEFAULT '0',`ticket` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,`run_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,"
				+"PRIMARY KEY (`id_cache_exec`), KEY `idx_exec_end_time` (`end_time`), KEY `idx_cache_exec_status` (`id_ref_cache_exec_status`),"
				+"KEY `idx_cache_cache_type` (`id_ref_cache_type`) "
				+") ENGINE=InnoDB AUTO_INCREMENT=5751741 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci range distribute on (id_ref_cache_type) using openrange");
		
		String sql = "select id_cache_exec, id_ref_cache_type, max(end_time) as max_end_time "
				+"from mydb.cache_exec where id_ref_cache_exec_status = 2 and id_ref_cache_type in (1,2) "
				+"and id_ref_cache_exec_type in (1,2) group by id_ref_cache_type order by max_end_time asc";
		stmtTest(db,sql,SelectStatement.class,
				null);
		
	}
	
	@Test
	public void testPE761_VarPop() throws Throwable {
		final SchemaContext db = buildSchema(TestName.MULTI,
				"CREATE TABLE `pe761` (`value` int) RANDOM DISTRIBUTE");
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		final String sql = "SELECT VAR_POP(`value`) FROM `pe761`";
		stmtTest(db, sql, SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
								group, "temp1", TransientExecutionEngine.AGGREGATION, StaticDistributionModel.MODEL_NAME,
								emptyDV,
								emptyIndexes,
								"SELECT SUM( `pe761`.`value` )  AS func_8,COUNT( `pe761`.`value` )  AS func_9",
								"FROM `pe761`"
						)
								.withExplain(new DMLExplainRecord(DMLExplainReason.AGGREGATION)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
								TransientExecutionEngine.AGGREGATION, "temp2", group, BroadcastDistributionModel.MODEL_NAME,
								emptyDV,
								emptyIndexes,
								"SELECT  (SUM( temp1.func_8 )  / SUM( temp1.func_9 ) )  AS func",
								"FROM temp1"
						)
								.withExplain(new DMLExplainRecord(DMLExplainReason.AGGREGATION)),
						new ProjectingExpectedStep(
								ExecutionType.SELECT,
								group,
								"temp3",
								TransientExecutionEngine.AGGREGATION,
								StaticDistributionModel.MODEL_NAME,
								emptyDV,
								emptyIndexes,
								"SELECT COUNT( `pe761`.`value` )  AS func_4,VAR_POP( `pe761`.`value` )  AS func_5,POW( AVG( `pe761`.`value` )  - temp2.func,2 )  AS func_6",
								"FROM `pe761`, temp2"
						)
								.withExplain(new DMLExplainRecord(DMLExplainReason.AGGREGATION)),
						new ProjectingExpectedStep(
								ExecutionType.SELECT,
								null,
								"SELECT  (SUM( temp3.func_4 * temp3.func_5 )  / SUM( temp3.func_4 )  + SUM( temp3.func_4 * temp3.func_6 )  / SUM( temp3.func_4 ) )  AS func",
								"FROM temp3"
						)
								.withExplain(new DMLExplainRecord(DMLExplainReason.AGGREGATION))
				));
	}
	
	@Test
	public void testPE761_VarSamp() throws Throwable {
		final SchemaContext db = buildSchema(TestName.MULTI,
				"CREATE TABLE `pe761` (`value` int) RANDOM DISTRIBUTE");
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		final String sql = "SELECT VAR_SAMP(`value`) FROM `pe761`";
		stmtTest(db, sql, SelectStatement.class, null);
	}

	@Test
	public void testPE761_StddevPop() throws Throwable {
		final SchemaContext db = buildSchema(TestName.MULTI,
				"CREATE TABLE `pe761` (`value` int) RANDOM DISTRIBUTE");
		final PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		final String sql = "SELECT STDDEV_POP(`value`) FROM `pe761`";
		stmtTest(db, sql, SelectStatement.class, null);
	}

}
