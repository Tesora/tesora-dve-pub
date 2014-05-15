// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class GroupByTransformTest extends TransformTest {

	public GroupByTransformTest() {
		super("GroupByTransformTest");
	}

	// our tiny little schema
	private static final String[] schema = new String[] {
		"create table A (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) static distribute on (`id`);",
		"create table B (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) broadcast distribute;",
		"create table R (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) random distribute;",
		"create range offsides (integer) persistent group g1;",
		"create table N (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) range distribute on (`id`) using offsides;"
	};

	// varieties of order by:
	// order by non ref col
	// order by ref col
	// order by ref expr (via alias instance)
	// order by non ref expr (expr in order by)
	// going to do all of these twice: once on the random table, and once on the bcast table
	@Test
	public void testNonRefColRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select desc from R group by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"R1i0_8" }, new String[][] {{"R1i0_8"} },
						  "SELECT `R`.`desc` AS R1d1_7,`R`.`id` AS R1i0_8",
						  "FROM `R`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.R1d1_7 AS t2R0,temp1.R1i0_8 AS t2R1",
						  "FROM temp1",
						  "GROUP BY t2R1 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.t2R0 AS t3t0",
						  "FROM temp2",
						  "ORDER BY temp2.t2R1 ASC"
						)
					)
					);
	}
	
	@Test
	public void testNonRefColBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select desc from B group by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`desc` AS B1d1 FROM `B` GROUP BY `B`.`id` ASC",
								null)));
	}
	
	@Test
	public void testRefColAliasRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id as me, desc from R group by me",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"me" }, new String[][] {{"me"} },
						  "SELECT `R`.`id` AS me,`R`.`desc` AS R1d1_5",
						  "FROM `R`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.me AS me,temp1.R1d1_5 AS t2R1",
						  "FROM temp1",
						  "GROUP BY me ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.me AS me,temp2.t2R1 AS t3t1",
						  "FROM temp2",
						  "ORDER BY me ASC"
						)
					)
				);
	}

	@Test
	public void testRefColAliasBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id as me, desc from B group by me",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS me,`B`.`desc` AS B1d1 FROM `B` GROUP BY me ASC",
								null)));
	}

	
	@Test
	public void testRefColRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id, desc from R group by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"R1i0_6" }, new String[][] {{"R1i0_6"} },
						  "SELECT `R`.`id` AS R1i0_6,`R`.`desc` AS R1d1_7",
						  "FROM `R`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.R1i0_6 AS t2R0,temp1.R1d1_7 AS t2R1",
						  "FROM temp1",
						  "GROUP BY t2R0 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.t2R0 AS t3t0,temp2.t2R1 AS t3t1",
						  "FROM temp2",
						  "ORDER BY t3t0 ASC"
						)
					)
					);
	}
	
	@Test
	public void testRefColBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id, desc from B group by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS B1i0,`B`.`desc` AS B1d1 FROM `B` GROUP BY B1i0 ASC",
								null)));
	}
	
	@Test
	public void testRefExprRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id,length(desc) as l from R group by l",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"l" }, new String[][] { },
						  "SELECT `R`.`id` AS R1i0_5,length( `R`.`desc` )  AS l",
						  "FROM `R`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.R1i0_5 AS t2R0,temp1.l AS l",
						  "FROM temp1",
						  "GROUP BY l ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.t2R0 AS t3t0,temp2.l AS l",
						  "FROM temp2",
						  "ORDER BY l ASC"
						)
					));
	}

	@Test
	public void testRefExprBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id,length(desc) as l from B group by l",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS B1i0,length( `B`.`desc` )  AS l FROM `B` GROUP BY l ASC",
								null)));
	}

	@Test
	public void testNonRefExprRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id, desc from R group by length(desc)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"func_12" }, new String[][] { },
						  "SELECT `R`.`id` AS R1i0_10,`R`.`desc` AS R1d1_11,length( `R`.`desc` )  AS func_12",
						  "FROM `R`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.R1i0_10 AS t2R0,temp1.R1d1_11 AS t2R1,temp1.func_12 AS t2f2",
						  "FROM temp1",
						  "GROUP BY t2f2 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.t2R0 AS t3t0,temp2.t2R1 AS t3t1",
						  "FROM temp2",
						  "ORDER BY temp2.t2f2 ASC"
						)
					));
	}

	@Test
	public void testNonRefExprBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id, desc from B group by length(desc)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS B1i0,`B`.`desc` AS B1d1 FROM `B` GROUP BY length( `B`.`desc` )  ASC",
								null)));
	}

	
	@Test
	public void testSimpleA() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"select desc from A group by flags",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"A1f2_8" }, new String[][] {{"A1f2_8"} },
						  "SELECT `A`.`desc` AS A1d1_7,`A`.`flags` AS A1f2_8",
						  "FROM `A`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.A1d1_7 AS t2A0,temp1.A1f2_8 AS t2A1",
						  "FROM temp1",
						  "GROUP BY t2A1 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.t2A0 AS t3t0",
						  "FROM temp2",
						  "ORDER BY temp2.t2A1 ASC"
						)
					)
					);
	}
	
	@Test
	public void testSimpleB() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"select desc from A group by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT `A`.`desc` AS A1d1_7,`A`.`id` AS A1i0_8",
						  "FROM `A`",
						  "GROUP BY A1i0_8 ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp1.A1d1_7 AS t2A0",
						  "FROM temp1",
						  "ORDER BY temp1.A1i0_8 ASC"
						)
					)
				);
	}
	
	@Test
	public void testComplexA() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select n.desc,r.desc from N n, R r where n.id = r.id group by r.flags",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT n.`desc` AS nd1_3,n.`id` AS ni0_4 FROM `N` AS n",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ni0_4" })
							),
							bes(
								new ProjectingExpectedStep(
								"SELECT r.`desc` AS rd1_4,r.`flags` AS rf2_5,r.`id` AS ri0_6 FROM `R` AS r",
									group,"temp2",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ri0_6" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.nd1_3 AS t3n0_11,temp2.rd1_4 AS t4r0_12,temp2.rf2_5 AS t4r1_13 FROM temp1, temp2 WHERE temp1.ni0_4 = temp2.ri0_6",
							TransientExecutionEngine.LARGE,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"t4r1_13" }),
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT temp3.t3n0_11 AS t5t0,temp3.t4r0_12 AS t5t1,temp3.t4r1_13 AS t5t2 FROM temp3 GROUP BY t5t2 ASC",
							TransientExecutionEngine.LARGE,"temp4",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp4.t5t0 AS t6t0,temp4.t5t1 AS t6t1 FROM temp4 ORDER BY temp4.t5t2 ASC",
							null)
					));
	}
	
	@Test
	public void testComplexB() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select n.desc,r.desc from N n, R r where n.id = r.id group by r.id",
				SelectStatement.class,
				bes(
						bpes(
								bes(
										new ProjectingExpectedStep(
												"SELECT n.`desc` AS nd1_3,n.`id` AS ni0_4 FROM `N` AS n",
												group,"temp1",TransientExecutionEngine.LARGE, StaticDistributionModel.MODEL_NAME,new String[] { "ni0_4" })),
								bes(
										new ProjectingExpectedStep(
												"SELECT r.`desc` AS rd1_3,r.`id` AS ri0_4 FROM `R` AS r",
												group,"temp2",TransientExecutionEngine.LARGE ,StaticDistributionModel.MODEL_NAME,new String[] { "ri0_4" }))
							),
							new ProjectingExpectedStep(ExecutionType.SELECT,
									"SELECT temp1.nd1_3 AS t3n0_9,temp2.rd1_3 AS t4r0_10,temp2.ri0_4 AS t4r1_11 FROM temp1, temp2 WHERE temp1.ni0_4 = temp2.ri0_4 GROUP BY t4r1_11 ASC",
										TransientExecutionEngine.LARGE,"temp3",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
							new ProjectingExpectedStep(
									"SELECT temp3.t3n0_9 AS t5t0,temp3.t4r0_10 AS t5t1 FROM temp3 ORDER BY temp3.t4r1_11 ASC",
									null)));
	}

	@Test
	public void testComplexC() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id, concat(desc, slug) as complete from A group by complete",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"complete" }, new String[][] { },
						  "SELECT `A`.`id` AS A1i0_5,concat( `A`.`desc`,`A`.`slug` )  AS complete",
						  "FROM `A`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.LARGE,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT temp1.A1i0_5 AS t2A0,temp1.complete AS complete",
						  "FROM temp1",
						  "GROUP BY complete ASC"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.t2A0 AS t3t0,temp2.complete AS complete",
						  "FROM temp2",
						  "ORDER BY complete ASC"
						)
					)
					);
	}	
	

}
