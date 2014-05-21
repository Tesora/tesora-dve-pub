// OS_STATUS: public
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
import com.tesora.dve.sql.util.TestName;

public class OrderByTransformTest extends TransformTest {
	
	public OrderByTransformTest() {
		super("OrderByTransformTest");
	}
	
	// our tiny little schema
	private static final String[] schema = new String[] {
		"create table A (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) static distribute on (`id`);",
		"create table B (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) broadcast distribute;",
		"create table R (`id` integer unsigned not null, `desc` varchar(50), flags tinyint, `slug` varchar(128)) random distribute;",
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
				"select desc from R order by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `R`.`desc` AS R1d1_5,`R`.`id` AS R1i0_6 FROM `R`",
								group,"temp1",TransientExecutionEngine.AGGREGATION, StaticDistributionModel.MODEL_NAME, new String[] {	}),
						new ProjectingExpectedStep(
								"SELECT temp1.R1d1_5 AS t2R0 FROM temp1 ORDER BY temp1.R1i0_6 ASC",
								null)));
	}

	@Test
	public void testNonRefColBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select desc from B order by id for update",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`desc` AS B1d1 FROM `B` ORDER BY `B`.`id` ASC FOR UPDATE", null)));
	}
	
	@Test
	public void testRefColAliasRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id as me, desc from R order by me",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `R`.`id` AS me,`R`.`desc` AS R1d1_4 FROM `R`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,
								StaticDistributionModel.MODEL_NAME, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT temp1.me AS me,temp1.R1d1_4 AS t2R1 FROM temp1 ORDER BY me ASC",
								null)));
	}

	@Test
	public void testRefColAliasBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id as me, desc from B order by me",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS me,`B`.`desc` AS B1d1 FROM `B` ORDER BY me ASC",
								null)));
	}

	
	@Test
	public void testRefColRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id, desc from R order by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `R`.`id` AS R1i0_4,`R`.`desc` AS R1d1_5 FROM `R`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] {}),
						new ProjectingExpectedStep(
								"SELECT temp1.R1i0_4 AS t2R0,temp1.R1d1_5 AS t2R1 FROM temp1 ORDER BY t2R0 ASC",
								null)));
	}
	
	@Test
	public void testRefColBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id, desc from B order by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS B1i0,`B`.`desc` AS B1d1 FROM `B` ORDER BY B1i0 ASC",
								null)));
	}
	
	@Test
	public void testRefExprRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id,length(desc) as l from R order by l",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `R`.`id` AS R1i0_4,length( `R`.`desc` )  AS l FROM `R`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
								"SELECT temp1.R1i0_4 AS t2R0,temp1.l AS l FROM temp1 ORDER BY l ASC",
								null)));
	}

	@Test
	public void testRefExprBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id,length(desc) as l from B order by l",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS B1i0,length( `B`.`desc` )  AS l FROM `B` ORDER BY l ASC",
								null)));
	}

	@Test
	public void testNonRefExprRandom() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id, desc from R order by length(desc)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `R`.`id` AS R1i0_7,`R`.`desc` AS R1d1_8,length( `R`.`desc` )  AS func_9 FROM `R`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
								"SELECT temp1.R1i0_7 AS t2R0,temp1.R1d1_8 AS t2R1 FROM temp1 ORDER BY temp1.func_9 ASC",
								null)));
	}

	@Test
	public void testNonRefExprBCast() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db,
				"select id, desc from B order by length(desc)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT `B`.`id` AS B1i0,`B`.`desc` AS B1d1 FROM `B` ORDER BY length( `B`.`desc` )  ASC",
								null)));
	}

	@Test
	public void testSimpleA() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db, 
				"select * from A where slug = 'your title here' order by desc",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT `A`.`id` AS A1i0_6,`A`.`desc` AS A1d1_7,`A`.`flags` AS A1f2_8,`A`.`slug` AS A1s3_9 FROM `A` WHERE `A`.`slug` = 'your title here'",
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] {}),
						new ProjectingExpectedStep(
						"SELECT temp1.A1i0_6 AS t2A0,temp1.A1d1_7 AS t2A1,temp1.A1f2_8 AS t2A2,temp1.A1s3_9 AS t2A3 FROM temp1 ORDER BY t2A1 ASC",
							null)
					));
	}
	
	@Test
	public void testSimpleB() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);		
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db, 
				"select id from A where slug = 'title' order by desc",
				SelectStatement.class,
				bes(
					new ProjectingExpectedStep(
							"SELECT `A`.`id` AS A1i0_5,`A`.`desc` AS A1d1_6 FROM `A` WHERE `A`.`slug` = 'title'",
							group,"temp1",TransientExecutionEngine.AGGREGATION,
							StaticDistributionModel.MODEL_NAME,
							new String[] {}),
					new ProjectingExpectedStep(
							"SELECT temp1.A1i0_5 AS t2A0 FROM temp1 ORDER BY temp1.A1d1_6 ASC",
							null)));
	}
	
	@Test
	public void testSimpleC() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db, 
				"select id from B where slug = 'title' order by desc",
				SelectStatement.class,
				bes(
					new ProjectingExpectedStep(
							"SELECT `B`.`id` AS B1i0 FROM `B` WHERE `B`.`slug` = 'title' ORDER BY `B`.`desc` ASC",
							null)));
	}
	
	@Test
	public void testSimpleD() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db, 
				"select id from R where slug = 'title' order by desc",
				SelectStatement.class,
				bes(
					new ProjectingExpectedStep(
							"SELECT `R`.`id` AS R1i0_5,`R`.`desc` AS R1d1_6 FROM `R` WHERE `R`.`slug` = 'title'",
							group,"temp1",TransientExecutionEngine.AGGREGATION,
							StaticDistributionModel.MODEL_NAME,
							new String[] {}),
					new ProjectingExpectedStep(
							"SELECT temp1.R1i0_5 AS t2R0 FROM temp1 ORDER BY temp1.R1d1_6 ASC",
							null)));
	}

	@Test
	public void testComplexA() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		stmtTest(db, 
				"select desc from A where id = 1 order by slug",
				SelectStatement.class,
				bes(
					new ProjectingExpectedStep(
							"SELECT `A`.`desc` AS A1d1_5 FROM `A` WHERE `A`.`id` = 1 ORDER BY `A`.`slug` ASC",
							null)));
	}

	@Test
	public void testComplexB() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select a.desc, r.desc from A a, R r where a.id = r.id order by r.id",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`desc` AS ad1_3,a.`id` AS ai0_4 FROM `A` AS a",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_4" })
							),
							bes(
								new ProjectingExpectedStep(
								"SELECT r.`desc` AS rd1_3,r.`id` AS ri0_4 FROM `R` AS r",
									group,"temp2",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ri0_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.ad1_3 AS t3a0_9,temp2.rd1_3 AS t4r0_10,temp2.ri0_4 AS t4r1_11 FROM temp1, temp2 WHERE temp1.ai0_4 = temp2.ri0_4",
							TransientExecutionEngine.LARGE,"temp3",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp3.t3a0_9 AS t5t0,temp3.t4r0_10 AS t5t1 FROM temp3 ORDER BY temp3.t4r1_11 ASC",
							null)
					));
	}

	@Test
	public void testMTDemoFailure() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,
				"create table mtdemo ( " 
				+ " `id` int, `pa` int, `pb` int, `___mtid` int, primary key (`id`, `___mtid`) ) static distribute on (`___mtid`)");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select mt.pa,mt.id from mtdemo mt order by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT mt.pa AS mtp1_4,mt.id AS mti0_5 FROM `mtdemo` AS mt WHERE mt.`___mtid` = 42",
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp1.mtp1_4 AS t2m0,temp1.mti0_5 AS t2m1 FROM temp1 ORDER BY t2m1 ASC",
							null)
					));
	}
	
	@Test
	public void testOrderByNull() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table obnt (`id` int, `pid` int, `sid` int, `junk` varchar(32), primary key (`id`))");
		stmtTest(db,
				"select max(o.pid), o.sid from obnt o group by o.sid having (max(o.pid) > 100) order by null",
				SelectStatement.class,
				null);
	}
	
	@Test
	public void testPE471() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe471 (`id` int, `fid` int, primary key (`id`))");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select id, fid from pe471 order by 1",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT `pe471`.`id` AS p1i0_4,`pe471`.`fid` AS p1f1_5 FROM `pe471`",
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp1.p1i0_4 AS t2p0,temp1.p1f1_5 AS t2p1 FROM temp1 ORDER BY t2p0 ASC",
							null)
					));
	}
	
}
