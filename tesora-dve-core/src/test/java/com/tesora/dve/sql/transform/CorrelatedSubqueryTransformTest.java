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
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class CorrelatedSubqueryTransformTest extends TransformTest {

	public CorrelatedSubqueryTransformTest() {
		super("CorrelatedSubqueryTest");
	}
	
	
	
	private static final String dataTestBody = 
			" `id` int auto_increment, `e` int, `d` int, `c` int, `b` int, `a` int, primary key (id) ";

	private static final String[] dataTestSchema = new String[] {
		"create table A (" + dataTestBody + ") random distribute",
		"create table B (" + dataTestBody + ") broadcast distribute",
		"create table S (" + dataTestBody + ") static distribute on (id)",
		"create table R (" + dataTestBody + ") range distribute on (id) using openrange"
	};
	
	@Test
	public void testProjA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, dataTestSchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		String sql = "select r.id, (select avg(l.c) from A l where l.b = r.c) from S r where r.c in (2,3) and r.e in (7,8)";
		stmtTest(db,sql,SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"rc3_6"} },
						  "SELECT r.`id` AS ri0_4,r.`c` AS rc3_6",
						  "FROM `S` AS r",
						  "WHERE r.`c` in ( 2,3 ) and r.`e` in ( 7,8 )"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.AGGREGATION,"temp2",group,BroadcastDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"t4r1"} },
						  "SELECT temp1.rc3_6 AS t4r1",
						  "FROM temp1"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.CORRELATED_SUBQUERY_LOOKUP_TABLE)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp3",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,
							new String[] {"lb4_10" },
							new String[][] {{"lb4_10"} },
						  "SELECT sum( l.`c` )  AS func_8,COUNT( l.`c` )  AS func_9,l.`b` AS lb4_10",
						  "FROM `A` AS l, temp2",
						  "WHERE l.`b` = temp2.t4r1",
						  "GROUP BY lb4_10 ASC"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.WRONG_DISTRIBUTION)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.MEDIUM,"temp4",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"t6l2_8"} },
						  "SELECT  (sum( temp3.func_8 )  / sum( temp3.func_9 ) )  AS func,temp3.lb4_10 AS t6l2_8",
						  "FROM temp3",
						  "GROUP BY t6l2_8 ASC"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.WRONG_DISTRIBUTION)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp1.ri0_4 AS t3r0,temp4.func AS subq",
						  "FROM temp1",
						  "LEFT OUTER JOIN temp4 ON  (temp4.t6l2_8 = temp1.rc3_6)"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.PROJECTION_CORRELATED_SUBQUERY))
					)
				);
	}

	
	@Test
	public void testPE1362A() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table OA (id int, fid int, sid int, primary key (id))",
				"create table IA (id int, fid int, sid int, primary key (id))",
				"create table IB (id int, fid int, sid int, primary key (id))",
				"create table IC (id int, fid int, sid int, primary key (id))");
		String sql =
				"select oa.fid, "
				+"((select max(ia.sid) from IA ia where ia.id = oa.id) + (select max(ib.sid) from IB ib where ib.id = oa.id)) + oa.sid pooched "
				+"from OA oa where oa.sid > 15";
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,sql,SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"oai0_11"} },
						  "SELECT oa.`fid` AS oaf1_5,oa.`sid` AS oas2_9,oa.`id` AS oai0_11",
						  "FROM `OA` AS oa",
						  "WHERE oa.`sid` > 15"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.AGGREGATION,"temp2",group,BroadcastDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"t5o2"} },
						  "SELECT temp1.oai0_11 AS t5o2",
						  "FROM temp1"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.CORRELATED_SUBQUERY_LOOKUP_TABLE)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp3",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,
							new String[] {"iai0_9" },
							new String[][] {{"iai0_9"} },
						  "SELECT max( ia.`sid` )  AS func_8,ia.`id` AS iai0_9",
						  "FROM `IA` AS ia, temp2",
						  "WHERE ia.`id` = temp2.t5o2",
						  "GROUP BY iai0_9 ASC"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.WRONG_DISTRIBUTION)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.MEDIUM,"temp4",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"t7i1_6"} },
						  "SELECT max( temp3.func_8 )  AS func,temp3.iai0_9 AS t7i1_6",
						  "FROM temp3",
						  "GROUP BY t7i1_6 ASC"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.WRONG_DISTRIBUTION)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp5",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,
							new String[] {"ibi0_9" },
							new String[][] {{"ibi0_9"} },
						  "SELECT max( ib.`sid` )  AS func_8,ib.`id` AS ibi0_9",
						  "FROM `IB` AS ib, temp2",
						  "WHERE ib.`id` = temp2.t5o2",
						  "GROUP BY ibi0_9 ASC"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.WRONG_DISTRIBUTION)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.MEDIUM,"temp6",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,
							emptyDV,
							new String[][] {{"t10i1_6"} },
						  "SELECT max( temp5.func_8 )  AS func,temp5.ibi0_9 AS t10i1_6",
						  "FROM temp5",
						  "GROUP BY t10i1_6 ASC"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.WRONG_DISTRIBUTION)),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp1.oaf1_5 AS t4o0, (temp4.func + temp6.func)  + temp1.oas2_9 AS func_9",
						  "FROM temp1",
						  "LEFT OUTER JOIN temp4 ON  (temp4.t7i1_6 = temp1.oai0_11)",
						  "LEFT OUTER JOIN temp6 ON  (temp6.t10i1_6 = temp1.oai0_11)"
						)
						.withExplain(new DMLExplainRecord(DMLExplainReason.PROJECTION_CORRELATED_SUBQUERY))
					)
					);
	}
	
	
}
