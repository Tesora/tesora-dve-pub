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

import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class UnionTransformTest extends TransformTest {

	public UnionTransformTest() {
		super("UnionTransformTest");
	}

	@Test
	public void testUnionA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table test (`id` int, `name` varchar(24), `age` int, `job` varchar(24) )");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select t.name as name from test t where (age in ('27', '28')) union select t.name as name from test t where (age = '28')",
				UnionStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT t.`name` AS name FROM `test` AS t WHERE  (t.`age` in ( '27','28' ))",
									group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] {})
							),
							bes(
								new ProjectingExpectedStep(
								"SELECT t.`name` AS name FROM `test` AS t WHERE  (t.`age` = '28')",
									group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] {})
							)
						),
						new ProjectingExpectedStep(ExecutionType.UNION,
						"SELECT temp1.name AS name FROM temp1 UNION SELECT temp2.name AS name FROM temp2",
							TransientExecutionEngine.AGGREGATION)
					));
	}

	@Test
	public void testUnionB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table test (`id` int, `name` varchar(24), `age` int, `job` varchar(24) )",
				"create table best (`id` int, `name` varchar(24), `age` int, `job` varchar(24) )");
		String sql = "select t.name as name from test t where (age in ('27', '28')) union all select t.name as name from best t where (age = '28')";
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				sql,
				UnionStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.UNION,
						"SELECT t.`name` AS name FROM `test` AS t WHERE  (t.`age` in ( '27','28' ))  UNION ALL  SELECT t.`name` AS name FROM `best` AS t WHERE  (t.`age` = '28')",
							group)
					));
	}

	@Test
	public void testUnionC() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table test (`id` int, `name` varchar(24)) broadcast distribute");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select t.name as name from test t where id in (1,2) union all select t.name as name from test t where id in (3)",
				UnionStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.UNION,
						"SELECT t.`name` AS name FROM `test` AS t WHERE t.`id` in ( 1,2 ) UNION ALL  SELECT t.`name` AS name FROM `test` AS t WHERE t.`id` in ( 3 )",
							group)
					));
	}
	
	@Test
	public void testUnionD() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table LA (id int, fid int, primary key (id))",
				"create table LB (id int, fid int, primary key (id))",
				"create table RA (id int, fid int, primary key (id))",
				"create table RB (id int, fid int, primary key (id))");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select la.id as id from LA la inner join LB lb on la.fid = lb.fid union all select ra.id as id from RA ra inner join RB rb on ra.fid = rb.fid",
				UnionStatement.class,
				bes(
						bpes(
							bes(
								bpes(
									bes(
										new ProjectingExpectedStep(ExecutionType.SELECT,
											group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"laf1_4" }, new String[][] {{"laf1_4"} },
										  "SELECT la.`id` AS lai0_3,la.`fid` AS laf1_4",
										  "FROM `LA` AS la"
										)
									),
									bes(
										new ProjectingExpectedStep(ExecutionType.SELECT,
											group,"temp2",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"lbf1_2" }, new String[][] {{"lbf1_2"} },
										  "SELECT lb.`fid` AS lbf1_2",
										  "FROM `LB` AS lb"
										)
									)
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.LARGE,"temp5",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp1.lai0_3 AS t5l0_7",
								  "FROM temp1",
								  "INNER JOIN temp2 ON temp1.laf1_4 = temp2.lbf1_2"
								)
							),
							bes(
								bpes(
									bes(
										new ProjectingExpectedStep(ExecutionType.SELECT,
											group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"raf1_4" }, new String[][] {{"raf1_4"} },
										  "SELECT ra.`id` AS rai0_3,ra.`fid` AS raf1_4",
										  "FROM `RA` AS ra"
										)
									),
									bes(
										new ProjectingExpectedStep(ExecutionType.SELECT,
											group,"temp4",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"rbf1_2" }, new String[][] {{"rbf1_2"} },
										  "SELECT rb.`fid` AS rbf1_2",
										  "FROM `RB` AS rb"
										)
									)
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.LARGE,"temp6",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp3.rai0_3 AS t7r0_7",
								  "FROM temp3",
								  "INNER JOIN temp4 ON temp3.raf1_4 = temp4.rbf1_2"
								)
							)
						),
						new ProjectingExpectedStep(ExecutionType.UNION,
							null,
						  "SELECT temp5.t5l0_7 AS t9t0",
						  "FROM temp5",
						  "UNION ALL",
						  "SELECT temp6.t7r0_7 AS t10t0",
						  "FROM temp6"
						)
					));
	}
	
	@Test
	public void test716A() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe716a (`id` int, `fid` int)",
				"create table pe716b (`id` int, `fid` int)");
		// this should not build a select step - this should be an adhoc step, but whatever for now
		stmtTest(db,
				"(select 'hello') order by 1",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT 'hello' AS litex ORDER BY litex ASC",
							null)
					));		
	}
	
	@Test
	public void test716B() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe716a (`id` int, `fid` int)",
				"create table pe716b (`id` int, `fid` int)");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"(select id from pe716a where fid > 10) union all (select id from pe716b where fid < 10) order by 1 limit 10",
				UnionStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT `pe716a`.`id` AS p1i0_4",
								  "FROM `pe716a`",
								  "WHERE `pe716a`.`fid` > 10",
								  "ORDER BY p1i0_4 ASC",
								  "LIMIT 10"
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.AGGREGATION,"temp3",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp1.p1i0_4 AS t3p0",
								  "FROM temp1",
								  "ORDER BY t3p0 ASC",
								  "LIMIT 10"
								)
							),
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT `pe716b`.`id` AS p2i0_4",
								  "FROM `pe716b`",
								  "WHERE `pe716b`.`fid` < 10",
								  "ORDER BY p2i0_4 ASC",
								  "LIMIT 10"
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.AGGREGATION,"temp4",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp2.p2i0_4 AS t4p0",
								  "FROM temp2",
								  "ORDER BY t4p0 ASC",
								  "LIMIT 10"
								)
							)
						),
						new ProjectingExpectedStep(ExecutionType.UNION,
							null,
						  "SELECT temp3.t3p0 AS t5t0",
						  "FROM temp3",
						  "UNION ALL",
						  "SELECT temp4.t4p0 AS t6t0",
						  "FROM temp4",
						  "ORDER BY 1 ASC",
						  "LIMIT 10"
						)
					)
				);
	}
	
	@Test
	public void test716C() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe716a (`id` int, `fid` int)",
				"create table pe716b (`id` int, `fid` int)");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"(select id from pe716a where fid > 10) union all (select fid from pe716b where id < 10) order by id limit 10",
				UnionStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT `pe716a`.`id` AS p1i0_4",
								  "FROM `pe716a`",
								  "WHERE `pe716a`.`fid` > 10",
								  "ORDER BY p1i0_4 ASC",
								  "LIMIT 10"
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.AGGREGATION,"temp3",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp1.p1i0_4 AS t3p0",
								  "FROM temp1",
								  "ORDER BY t3p0 ASC",
								  "LIMIT 10"
								)
							),
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT `pe716b`.`fid` AS p2f1_4",
								  "FROM `pe716b`",
								  "WHERE `pe716b`.`id` < 10",
								  "ORDER BY p2f1_4 ASC",
								  "LIMIT 10"
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.AGGREGATION,"temp4",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp2.p2f1_4 AS t4p0",
								  "FROM temp2",
								  "ORDER BY t4p0 ASC",
								  "LIMIT 10"
								)
							)
						),
						new ProjectingExpectedStep(ExecutionType.UNION,
							null,
						  "SELECT temp3.t3p0 AS t5t0",
						  "FROM temp3",
						  "UNION ALL",
						  "SELECT temp4.t4p0 AS t6t0",
						  "FROM temp4",
						  "ORDER BY 1 ASC",
						  "LIMIT 10"
						)
					)
				);
	}
	
	@Test
	public void test716D() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe716a (`id` int, `fid` int)",
				"create table pe716b (`id` int, `fid` int)");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"(select id as twit from pe716a where fid > 10) union all (select fid from pe716b where id < 10) order by twit limit 10",
				UnionStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT `pe716a`.`id` AS twit",
								  "FROM `pe716a`",
								  "WHERE `pe716a`.`fid` > 10",
								  "ORDER BY twit ASC",
								  "LIMIT 10"
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.AGGREGATION,"temp3",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp1.twit AS twit",
								  "FROM temp1",
								  "ORDER BY twit ASC",
								  "LIMIT 10"
								)
							),
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
									group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT `pe716b`.`fid` AS p2f1_4",
								  "FROM `pe716b`",
								  "WHERE `pe716b`.`id` < 10",
								  "ORDER BY p2f1_4 ASC",
								  "LIMIT 10"
								),
								new ProjectingExpectedStep(ExecutionType.SELECT,
									TransientExecutionEngine.AGGREGATION,"temp4",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
								  "SELECT temp2.p2f1_4 AS t4p0",
								  "FROM temp2",
								  "ORDER BY t4p0 ASC",
								  "LIMIT 10"
								)
							)
						),
						new ProjectingExpectedStep(ExecutionType.UNION,
							null,
						  "SELECT temp3.twit AS twit",
						  "FROM temp3",
						  "UNION ALL",
						  "SELECT temp4.t4p0 AS t6t0",
						  "FROM temp4",
						  "ORDER BY 1 ASC",
						  "LIMIT 10"
						)
					)
					);
	}
	
    @Test
    public void testPE881_simplified() throws Throwable {
        SchemaContext db = buildSchema(TestName.MULTI,
                "CREATE TABLE `foo` (`id` int(10) unsigned NOT NULL,`status` varchar(32) NOT NULL DEFAULT '')",
                "CREATE TABLE `bar` (`id` int(10) unsigned NOT NULL,`status` varchar(32) NOT NULL DEFAULT '')"
        );

        String sql = "(SELECT NULL as fid,foo.status AS 'fss' FROM foo) UNION (SELECT bar.id as `bid`, NULL as 'bstatus' FROM bar)";

        PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);

        stmtTest(db,
                sql,
                UnionStatement.class,
                bes(
                        bpes(
                                bes(
                                        new ProjectingExpectedStep(ExecutionType.SELECT,
                                                "SELECT NULL AS fid,`foo`.`status` AS `fss` FROM `foo`",
                                                group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { })
                                ),
                                bes(
                                        new ProjectingExpectedStep(ExecutionType.SELECT,
                                                "SELECT `bar`.`id` AS `bid`,NULL AS `bstatus` FROM `bar`",
                                                group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { })
                                )
                        ),
                        new ProjectingExpectedStep(ExecutionType.UNION,
                                "SELECT temp1.fid AS fid,temp1.`fss` AS `fss` FROM temp1 UNION SELECT temp2.`bid` AS `bid`,temp2.`bstatus` AS `bstatus` FROM temp2",
                                null)
                )
        );

    }

}
