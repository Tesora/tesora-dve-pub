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


import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

import org.junit.Test;

public class MultijoinTransformTest extends TransformTest {

	public MultijoinTransformTest() {
		super("MultijoinTransformTest");
	}

	// TODO:
	// should add the plans here - but will do so after constraint trees are added
	private static final String[] threeRandomTables = new String[] {
		"create table A (`id` int, `fid` int, `sid` int, `payload` varchar(32), key (fid), primary key (id))",
		"create table B (`id` int, `fid` int, `sid` int, `payload` varchar(32), unique key (fid), primary key (id))",
		"create table C (`id` int, `fid` int, `sid` int, `payload` varchar(32), primary key (id))"
	};
	
	@Test
	public void testMultiJoinA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, threeRandomTables);
		String sql =
				"select a.payload "
				+"from A a inner join B b on a.id=b.id "
				+"inner join C c on c.fid=a.fid and c.sid=b.sid "
				+"where c.id = 15";
		stmtTest(db,
				sql,
				SelectStatement.class,
				null);
	}
	
	@Test
	public void testMultiJoinB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, threeRandomTables);
		String sql =
				"select a.payload "
				+"from A a inner join B b on a.id=b.id "
				+"inner join C c on c.fid=a.fid and c.sid=b.sid "
				+"where b.id = 15";
		stmtTest(db,
				sql,
				SelectStatement.class,
				null);
	}

	@Test
	public void testMultiJoinC() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, threeRandomTables);
		String sql =
				"select a.payload "
				+"from A a inner join B b on a.id=b.id "
				+"inner join C c on c.fid=a.fid and c.sid=b.sid "
				+"where a.id = 15";
		stmtTest(db,
				sql,
				SelectStatement.class,
				null);
	}

	@Test
	public void testMultiJoinD() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, threeRandomTables);
		String sql =
				"select a.payload "
				+"from A a inner join B b on a.id=b.id "
				+"left outer join C c on c.fid=a.fid and c.sid=b.sid "
				+"where a.id = 15";
		stmtTest(db,
				sql,
				SelectStatement.class,
				null);
	}

	@Test
	public void testMultiJoinE() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, threeRandomTables);
		String sql =
				"select a.payload "
				+"from A a inner join B b on a.id=b.id "
				+"left outer join C c on c.fid=a.fid and c.sid=b.sid "
				+"where c.id = 15";
		stmtTest(db,
				sql,
				SelectStatement.class,
				null);
	}
	
    @Test
    public void testPE938_lookupJoinsTrackIndices() throws Throwable {
        SchemaContext db = buildSchema(TestName.MULTI, threeRandomTables);
        PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
        String sql =
                "select a.payload, b.payload "
                        +"from A a inner join B b on a.id=b.fid "
                        +"where a.fid = 15";//to hit lookup strategy, one side must be unconstrained, other must be constrained, but not unique
        stmtTest(db,
                sql,
                SelectStatement.class,
                bes(
                		bpes(
                			bes(
                				new ProjectingExpectedStep(ExecutionType.SELECT,
                					group,"temp1",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,
                					new String[] {"ai0_4" },
                					new String[][] {{"ai0_4"} },
                				  "SELECT a.`payload` AS ap3_3,a.`id` AS ai0_4",
                				  "FROM `A` AS a",
                				  "WHERE a.`fid` = 15"
                				),
                				new ProjectingExpectedStep(ExecutionType.SELECT,
                					TransientExecutionEngine.MEDIUM,"temp2",group,BroadcastDistributionModel.MODEL_NAME,
                					emptyDV,
                					new String[][] {{"t3a1"} },
                				  "SELECT DISTINCT temp1.ai0_4 AS t3a1",
                				  "FROM temp1"
                				)
                				.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE)),
                				new ProjectingExpectedStep(ExecutionType.SELECT,
                					group,"temp3",TransientExecutionEngine.MEDIUM,StaticDistributionModel.MODEL_NAME,
                					new String[] {"bf1_7" },
                					new String[][] {{"bf1_7"} },
                				  "SELECT b.`payload` AS bp3_6,b.`fid` AS bf1_7",
                				  "FROM `B` AS b, temp2",
                				  "WHERE temp2.t3a1 = b.`fid`"
                				)
                				.withExplain(new DMLExplainRecord(DMLExplainReason.LOOKUP_JOIN))
                			)
                		),
                		new ProjectingExpectedStep(ExecutionType.SELECT,
                			null,
                		  "SELECT temp1.ap3_3 AS t5a0_9,temp3.bp3_6 AS t6b0_10",
                		  "FROM temp1",
                		  "INNER JOIN temp3 ON temp1.ai0_4 = temp3.bf1_7"
                		)
                		.withExplain(new DMLExplainRecord(DMLExplainReason.ONE_SIDE_CONSTRAINED))
                	)                
        );
    }
	
}
