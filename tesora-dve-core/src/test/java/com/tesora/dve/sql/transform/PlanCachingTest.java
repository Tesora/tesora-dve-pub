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
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class PlanCachingTest extends TransformTest {

	public PlanCachingTest() {
		super("PlanCachingTest");
	}

	@Test
	public void testNoLiterals() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testa (`id` int auto_increment, `payload` varchar(32), primary key (`id`)) static distribute on (`id`)"
				);
		String sql = "select * from testa";
		ExpectedSequence es = 
			bes(
					new ProjectingExpectedStep(
					"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa`",
						null)
				);

		cachePlanTest(db,sql,false,es);
		cachePlanTest(db,sql,true,es);
	}
	
	@Test
	public void testOneLiteral() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testa (`id` int auto_increment, `payload` varchar(32), primary key (`id`)) static distribute on (`id`)"
				);
		cachePlanTest(db,"select * from testa where id = 15",false, 
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`id` = 15",
							null)
					));
		cachePlanTest(db,"select * from testa where id = 2000", true,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`id` = 2000",
							null)));
	}
	
	@Test
	public void testMultiLiteralA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testa (`id` int auto_increment, `payload` varchar(32), primary key (`id`)) static distribute on (`id`)"
				);
		cachePlanTest(db,"select * from testa where id in (15,16)",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE  (`testa`.`id` = 15 OR `testa`.`id` = 16)",
							null)
					));
		cachePlanTest(db,"select * from testa where id in (1,2)",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE  (`testa`.`id` = 1 OR `testa`.`id` = 2)",
							null)
					));	
	}
	
	@Test
	public void testMultiLiteralB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testa (`id` int auto_increment, `payload` varchar(32), primary key (`id`)) static distribute on (`id`)"
				);
		cachePlanTest(db,"select * from testa where payload in ('a','b')",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`payload` in ( 'a','b' )",
							null)
					));
		cachePlanTest(db,"select * from testa where payload in ('c','d')",true,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`payload` in ( 'c','d' )",
							null)
					));
	}
	
	@Test
	public void testMultiLiteralC() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testa (`id` int auto_increment, `payload` varchar(32), primary key (`id`)) static distribute on (`id`)"
				);
		cachePlanTest(db,"select * from testa where payload in ('a','b')",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`payload` in ( 'a','b' )",
							null)
					));
		cachePlanTest(db,"select * from testa where payload in ('c','d','e')",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`payload` in ( 'c','d','e' )",
							null)
					));
	}
	
	@Test
	public void testNonMatchingA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testa (`id` int auto_increment, `payload` varchar(32), primary key (`id`)) static distribute on (`id`)"
				);
		cachePlanTest(db,"select * from testa where payload in ('a','b')",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`payload` in ( 'a','b' )",
							null)
					));
		cachePlanTest(db,"select id from testa where payload in ('a','b')",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_3 FROM `testa` WHERE `testa`.`payload` in ( 'a','b' )",
							null)
					));
	}

	@Test
	public void testNonMatchingB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testa (`id` int auto_increment, `payload` varchar(32), primary key (`id`)) static distribute on (`id`)"
				);
		cachePlanTest(db,"select * from testa where payload in ('a','b')",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`payload` in ( 'a','b' )",
							null)
					));
		cachePlanTest(db,"select * from testa where payload in (1,2)",false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testa`.`id` AS t1i0_4,`testa`.`payload` AS t1p1_5 FROM `testa` WHERE `testa`.`payload` in ( 1,2 )",
							null)));
	}
	
	@Test
	public void testUpdateA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testb (`id` int auto_increment, `k` int, primary key (`id`)) static distribute on (`id`)"
				);
		cachePlanTest(db,"update testb set k=k+1 where id = 1",false,null);
		cachePlanTest(db,"update testb set k=k+2 where id = 2",true,null);
	}
	
	@Test
	public void testRedistA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testb (`id` int auto_increment, `k` int, primary key (`id`)) static distribute on (`id`)"
				);
		PEStorageGroup group = db.getCurrentPEDatabase().getDefaultStorage(db);
		cachePlanTest(db,
				"select count(*) from testb",
				false,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT COUNT( * )  AS func_4",
						  "FROM `testb`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT CONVERT( SUM( temp1.func_4 ) ,SIGNED )  AS func",
						  "FROM temp1"
						)
					)
					);
		cachePlanTest(db,
				"select count(*) from testb",
				true,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT COUNT( * )  AS func_4",
						  "FROM `testb`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT CONVERT( SUM( temp2.func_4 ) ,SIGNED )  AS func",
						  "FROM temp2"
						)
					)
					);
	}
	
	@Test
	public void testHexLiteralsA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table testhl (`id` int auto_increment, `hl` binary(24), primary key (`id`)) random distribute");
		//PEStorageGroup group = db.getCurrentPEDatabase().getDefaultStorage(db);
		cachePlanTest(db,
				"select id from testhl where hl = x'123456' or hl = x'789ABC'",
				false,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testhl`.`id` AS t1i0_3 FROM `testhl` WHERE `testhl`.`hl` = x'123456' or `testhl`.`hl` = x'789ABC'",
							null)
					));
		cachePlanTest(db,
				"select id from testhl where hl = x'CBA987' or hl = x'654321'",
				true,
				bes(
						new ProjectingExpectedStep(
						"SELECT `testhl`.`id` AS t1i0_3 FROM `testhl` WHERE `testhl`.`hl` = x'CBA987' or `testhl`.`hl` = x'654321'",
							null)
					));
	}
	
	@Test
	public void testPE1078() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table foo (id int, fid int, sid int, primary key (id)) random distribute");
		PEStorageGroup group = db.getCurrentPEDatabase().getDefaultStorage(db);
		cachePlanTest(db,
				"select fid from foo where id = 15 limit 25 offset 10",
				false,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT `foo`.`fid` AS f1f1_3",
						  "FROM `foo`",
						  "WHERE `foo`.`id` = 15",
						  "LIMIT 35"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp1.f1f1_3 AS t2f0",
						  "FROM temp1",
						  "LIMIT 10, 25"
						)
					)
					);
		cachePlanTest(db,
				"select fid from foo where id = 15 limit 100 offset 50",
				true,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT `foo`.`fid` AS f1f1_3",
						  "FROM `foo`",
						  "WHERE `foo`.`id` = 15",
						  "LIMIT 150"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp2.f1f1_3 AS t2f0",
						  "FROM temp2",
						  "LIMIT 50, 100"
						)
					)
					);
	}
	
	@Test
	public void testPE1174A() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI);
		cachePlanTest(db,"set @foo = 1",false,
				bes(new SessionVariableExpectedStep("USER","foo","1")));
		cachePlanTest(db,"set @foo = 15",true,
				bes(new SessionVariableExpectedStep("USER","foo","15")));
		cachePlanTest(db,"set time_zone = '00:00'",false,
				bes(new SessionVariableExpectedStep("SESSION","time_zone","00:00")));
		cachePlanTest(db,"set time_zone = '12:00'",true,
				bes(new SessionVariableExpectedStep("SESSION","time_zone","12:00")));
	}
	
	@Test
	public void testPE1147B() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI);
		PEStorageGroup group = db.getCurrentPEDatabase().getDefaultStorage(db);
		cachePlanTest(db,"begin",false,
				bes(new TransactionExpectedStep(group,"START TRANSACTION")));
		cachePlanTest(db,"commit",false,
				bes(new TransactionExpectedStep(group,"COMMIT")));
		cachePlanTest(db,"begin",true,
				bes(new TransactionExpectedStep(group,"START TRANSACTION")));
		cachePlanTest(db,"commit",true,
				bes(new TransactionExpectedStep(group,"COMMIT")));
	}
	
	@Test
	public void testPE1147C() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI);
		PEStorageGroup group = db.getCurrentPEDatabase().getDefaultStorage(db);
		cachePlanTest(db,"begin work",false,
				bes(new TransactionExpectedStep(group,"START TRANSACTION")));
		cachePlanTest(db,"commit work",false,
				bes(new TransactionExpectedStep(group,"COMMIT")));
		cachePlanTest(db,"begin work",true,
				bes(new TransactionExpectedStep(group,"START TRANSACTION")));
		cachePlanTest(db,"commit work",true,
				bes(new TransactionExpectedStep(group,"COMMIT")));
	}	
	
	@Test
	public void testPE1147D() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI);
		cachePlanTest(db,"set sql_mode=ifnull(@old_sql_mode,'')",false,
				bes(new FilterExpectedStep("SetVariableOperationFilter(@@session.sql_mode)",new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT ifnull( null,'' )",null))));
		cachePlanTest(db,"set foreign_key_checks=if(@old_foreign_key_checks=0,0,1)",false,
				bes(new FilterExpectedStep("SetVariableOperationFilter(@@session.foreign_key_checks)",new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT if( null = 0,0,1 )",null))));
		cachePlanTest(db,"set @old_sql_mode='foobar'",false,
				bes(new SessionVariableExpectedStep("USER","old_sql_mode","foobar")));
		// note this is not correct - but transient mode doesn't dispatch
		cachePlanTest(db,"set sql_mode=ifnull(@old_sql_mode,'')",true,
				bes(new FilterExpectedStep("SetVariableOperationFilter(@@session.sql_mode)",new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT ifnull( null,'' )",null))));
	}
	
	@Test
	public void testPE1147E() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI);
		cachePlanTest(db,"set @myvar=if(@sql_mode=0,1,2)",false,
				bes(new FilterExpectedStep("SetVariableOperationFilter(@myvar)",new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT if( null = 0,1,2 )",null))));
		cachePlanTest(db,"set @myvar=if(@sql_mode=0,2,4)",true,
				bes(new FilterExpectedStep("SetVariableOperationFilter(@myvar)",new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT if( null = 0,2,4 )",null))));
	}
}
