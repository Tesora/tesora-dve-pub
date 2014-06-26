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

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class MultitenantTransformTest extends TransformTest {

	public MultitenantTransformTest() {
		super("MultitenantTransformTest");
		// TODO Auto-generated constructor stub
	}

	// our tiny little schema.  we need four tables, A, B, C, D.
	// two tables dist on tenant column
	// one table dist on some other column
	// one table dist on tenant and other column
	// we need to test joins
	// {inner,outer} colocated on tenant column
	// {inner,outer} colocated on other column
	// {inner,outer} colocated on tenant and other
	// in all cases tenant column is not specified in query
	private static final String[] schema = new String[] {
		"create range rot (int) persistent group g1;",
		"create range ros (int, int) persistent group g1;",
		"create table A (`aid` integer, `desc` varchar(32), `slug` varchar(16), `___mtid` int) range distribute on (`___mtid`) using rot;",
		"create table B (`bid` integer, `desc` varchar(32), `slug` varchar(16), `___mtid` int) range distribute on (`___mtid`) using rot;",
		"create table C (`cid` integer, `desc` varchar(32), `slug` varchar(16), `___mtid` int) random distribute;",
		"create table D (`did` int, `desc` varchar(32), `slug` varchar(16), `___mtid` int) range distribute on (`___mtid`,`did`) using ros;"		
	};

	@Test
	public void testA() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		stmtTest(db,"select desc from A",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep("SELECT `A`.`desc` AS A1d1 FROM `A` WHERE `A`.`___mtid` = 42",null)
				));
	}

	private SchemaContext clearTenantID(SchemaContext db) throws Exception {
		TransientExecutionEngine tee = (TransientExecutionEngine) db.getCatalog();
		tee.parse(new String[] { "use " + PEConstants.LANDLORD_TENANT });
		return SchemaContext.createContext(tee, tee);
	}
	
	@Test
	public void testAW() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		db = clearTenantID(db);
		stmtTest(db,"select desc from A",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep("SELECT `A`.`desc` AS A1d1_3 FROM `A`",null)
				));		
	}
	
	@Test
	public void testB() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		stmtTest(db,"select a.desc from A a where a.aid in (1,2,3)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep("SELECT a.desc AS ad1 FROM `A` AS a WHERE  (a.aid in ( 1,2,3 ))  AND a.`___mtid` = 42",
								null)
				));
	}
	
	@Test
	public void testBW() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		db = clearTenantID(db);
		stmtTest(db,"select a.desc from A a where a.aid in (1,2,3)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep("SELECT a.`desc` AS ad1_3 FROM `A` AS a WHERE a.`aid` in ( 1,2,3 )",
								null)
				));
	}
	
	@Test
	public void testC() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		stmtTest(db,"select a.*, b.slug from A a, B b where a.aid = b.bid",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT a.`aid` AS aa0,a.`desc` AS ad1,a.`slug` AS as2,b.slug AS bs2",
						  "FROM `A` AS a, `B` AS b",
						  "WHERE  (a.aid = b.bid)  AND a.`___mtid` = 42 AND b.`___mtid` = 42"
						)
					)
				);
	}

	@Test
	public void testD() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		stmtTest(db,"select a.*, b.slug from A a left outer join B b on a.aid = b.bid where a.slug like '%wonderment%'",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT a.`aid` AS aa0,a.`desc` AS ad1,a.`slug` AS as2,b.slug AS bs2",
						  "FROM `A` AS a",
						  "LEFT OUTER JOIN `B` AS b ON a.`___mtid` = b.`___mtid` AND a.aid = b.bid",
						  "WHERE  (a.slug like '%wonderment%')  AND a.`___mtid` = 42"
						)
					)
				);				
	}

	@Ignore
	@Test
	public void testDW() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		db = clearTenantID(db);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"select a.*, b.slug from A a left outer join B b on a.aid = b.bid where a.slug like '%wonderment%'",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`aid` AS aa0_5,a.`desc` AS ad1_6,a.`slug` AS as2_7,a.`___mtid` AS a_3_8 FROM A AS a WHERE a.`slug` like '%wonderment%'",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"aa0_5" })
							),
							bes(
								new ProjectingExpectedStep(
								"SELECT b.slug AS bs2_3,b.bid AS bb0_4 FROM B AS b",
									group,"temp2",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"bb0_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.aa0_5 AS t3a0_13,temp1.ad1_6 AS t3a1_14,temp1.as2_7 AS t3a2_15,temp1.a_3_8 AS t3a3_16,temp2.bs2_3 AS t4b0_17 FROM temp1 LEFT OUTER JOIN temp2 ON temp1.aa0_5 = temp2.bb0_4",
							null)
					)				
					);
	}

	
	@Test
	public void testE() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"select a.*, c.slug from A a, C c where a.aid = c.cid",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`aid` AS aa0_4,a.`desc` AS ad1_5,a.`slug` AS as2_6 FROM `A` AS a WHERE a.`___mtid` = 42",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"aa0_4" })
							),
							bes(
								new ProjectingExpectedStep(
								"SELECT c.slug AS cs2_3,c.cid AS cc0_4 FROM `C` AS c WHERE c.`___mtid` = 42",
									group,"temp2",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"cc0_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.aa0_4 AS t3a0_11,temp1.ad1_5 AS t3a1_12,temp1.as2_6 AS t3a2_13,temp2.cs2_3 AS t4c0_14 FROM temp1, temp2 WHERE  (temp1.aa0_4 = temp2.cc0_4)",
							null)
					));
	}
	
	@Test
	public void testF() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		stmtTest(db,"select d.slug from D d where d.did = 4",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT d.slug AS ds2_3 FROM `D` AS d WHERE  (d.did = 4 AND d.`___mtid` = 42)",
								null)));
	}
	
	@Test
	public void testG() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"select d.slug from `D` d where d.did in (5,6,7)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT d.slug AS ds2_3 FROM `D` AS d WHERE  ( (d.did = 5 AND d.`___mtid` = 42)  OR  (d.did = 6 AND d.`___mtid` = 42)  OR  (d.did = 7 AND d.`___mtid` = 42) )",
							group)
					));
	}
	
	@Test
	public void testH() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"update D set slug = 'snail' where did in (5,6,7)",
				UpdateStatement.class,
				bes(
						new UpdateExpectedStep(group,
						"UPDATE `D` SET `D`.`slug` = 'snail' WHERE  ( (`D`.`did` = 5 AND `D`.`___mtid` = 42)  OR  (`D`.`did` = 6 AND `D`.`___mtid` = 42)  OR  (`D`.`did` = 7 AND `D`.`___mtid` = 42) )")
					));
	}
	
	@Test
	public void testI() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"delete from D where did in (5,6,7)",
				DeleteStatement.class,
				bes(
						new DeleteExpectedStep(group,
						"DELETE `D` FROM `D` WHERE  ( (`D`.`did` = 5 AND `D`.`___mtid` = 42)  OR  (`D`.`did` = 6 AND `D`.`___mtid` = 42)  OR  (`D`.`did` = 7 AND `D`.`___mtid` = 42) )")
					));
	}
	
	@Test
	public void testJ() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,"insert into C (`cid`, `desc`, `slug`) select b.bid as cid, b.desc as desc, b.slug as slug from B b where b.bid in (5,6,7)",
				InsertIntoSelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT b.bid AS `cid`,b.desc AS `desc`,b.slug AS `slug`,42 AS `___mtid` FROM `B` AS b WHERE  (b.bid in ( 5,6,7 ))  AND b.`___mtid` = 42",
								group,"C",group,RANDOM,new String[] { })));
	}

	@Test
	public void testK() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		// PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage();
		// PEStorageGroup temp = db.getCurrentDatabase().getTempStorage();
		stmtTest(db,"select count(*) as expression from (select 1 as expression from D d where d.did = 1) subquery",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT count( * )  AS func FROM `D` AS d WHERE  (d.did = 1 AND d.`___mtid` = 42)",
								null)));
	}
	@Test
	public void testCount() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		stmtTest(db,
				"select count(*) from A",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT count( * )  AS func FROM `A` WHERE `A`.`___mtid` = 42",
								null)));
	}
	
	@Test
	public void testCountW() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTIMT,schema);
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		db = clearTenantID(db);
		stmtTest(db,
				"select count(*) from A",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
								"SELECT count( * )  AS func_4 FROM `A`",
								group,"temp1",TransientExecutionEngine.AGGREGATION,STATIC, new String[] {}),
						new ProjectingExpectedStep(
								"SELECT CONVERT( sum( temp1.func_4 ) ,SIGNED )  AS func FROM temp1",
								null)));
	}

	/**
	 * Testing the parser only. As we currently do not support PARTITIONS and
	 * NDB files, the queries return empty sets.
	 */
	@Test
	public void testPE1153() throws Exception {
		final SchemaContext db = buildSchema(TestName.MULTIMT, schema);

		stmtTest(
				db,
				"SELECT LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA FROM INFORMATION_SCHEMA.FILES WHERE FILE_TYPE = 'UNDO LOG' AND FILE_NAME IS NOT NULL AND LOGFILE_GROUP_NAME IN (SELECT DISTINCT LOGFILE_GROUP_NAME FROM INFORMATION_SCHEMA.FILES WHERE FILE_TYPE = 'DATAFILE' AND TABLESPACE_NAME IN (SELECT DISTINCT TABLESPACE_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA IN ('"
						+ PEConstants.LANDLORD_TENANT
						+ "'))) GROUP BY LOGFILE_GROUP_NAME, FILE_NAME, ENGINE ORDER BY LOGFILE_GROUP_NAME",
				SelectStatement.class, null);

		stmtTest(
				db,
				"SELECT DISTINCT TABLESPACE_NAME, FILE_NAME, LOGFILE_GROUP_NAME, EXTENT_SIZE, INITIAL_SIZE, ENGINE FROM INFORMATION_SCHEMA.FILES WHERE FILE_TYPE = 'DATAFILE' AND TABLESPACE_NAME IN (SELECT DISTINCT TABLESPACE_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA IN ('"
						+ PEConstants.LANDLORD_TENANT
						+ "')) ORDER BY TABLESPACE_NAME, LOGFILE_GROUP_NAME;",
				SelectStatement.class, null);
	}
}
