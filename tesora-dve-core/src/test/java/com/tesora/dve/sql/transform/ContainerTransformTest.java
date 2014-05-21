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

import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class ContainerTransformTest extends TransformTest {

	public ContainerTransformTest() {
		super("ContainerTransformTest");
	}

	private static final String[] basicSchema = new String[] {
		"create range cont_range (int) persistent group g1",
		"create container testcont persistent group g1 range distribute using cont_range",
		"create table bt (`id` int, `junk` varchar(32), primary key (id)) discriminate on (id,junk) using container testcont",
		"create table A (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont",
		"create table B (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont",
		"create table NR (`id` int, `junk` varchar(32), primary key (id)) random distribute",
		"create table NB (`id` int, `junk` varchar(32), primary key (id)) broadcast distribute",
		"using container testcont (global)",
		"insert into bt values (1,'one')",
		"insert into bt values (2,'two')"
	};
	
	private static final Map<String,Object> firstContainer = buildFakeKey(new Object[] { "___mtid", new Long(1) });
	private static final Map<String,Object> secondContainer = buildFakeKey(new Object[] { "___mtid", new Long(2) });
	
	private SchemaContext buildBasicSchema(String ...extras) throws Throwable {
		ArrayList<String> commands = new ArrayList<String>(Arrays.asList(basicSchema));
		for(String s : extras)
			commands.add(s);
		return buildSchema(TestName.MULTI, commands.toArray(new String[0]));
	}
	
	// so it's not clear what the behavior is supposed to be here
	// I think it goes like this (assuming only joins)
	// [1] cont specified, only cont tables - dist key is set
	// [2] global cont specified, only cont table - no redist, but dist key not set
	// [3] cont specified, mixed tables - dist key set unless redist, redist (maybe)
	// [4] global cont specified, mixed tables -  dist key not set, redist (maybe)
	// [5] no cont specified, only cont tables - follow underlying rules (so this is not the single site rewrite)
	// [6] no cont specified, mixed tables - follow underlying rules
	
	
	@Test
	public void specCont_ContOnly() throws Throwable {
		// dist key should be set
		SchemaContext db = buildBasicSchema("using container testcont (1, 'one')");
		stmtTest(db,
				"/* specCont_ContOnly */select a.* from A a, B b where a.junk = b.junk and a.id = 2",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT a.`id` AS ai0,a.`junk` AS aj1",
						  "FROM `A` AS a, `B` AS b",
						  "WHERE a.`junk` = b.`junk` and a.`id` = 2"
						).withFakeKey(firstContainer)
					));
	}
	
	@Test
	public void globalCont_ContOnly() throws Throwable {
		// dist key should not be set
		SchemaContext db = buildBasicSchema();
		stmtTest(db,
				"/* globalCont_ContOnly */select a.* from A a, B b where a.id = b.id and a.junk = 'foo'",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT a.`id` AS ai0_5,a.`junk` AS aj1_6 FROM `A` AS a, `B` AS b WHERE a.`id` = b.`id` and a.`junk` = 'foo'",
						null)
						.withFakeKey(NULL_FAKE_KEY)
					));
	}
	
	@Test
	public void specCont_MixedBCast() throws Throwable {
		// dist key should be set - no redist
		SchemaContext db = buildBasicSchema("using container testcont (1, 'one')");
		stmtTest(db,
				"/* specCont_MixedBCast */select a.* from A a, NB b where a.id = b.id and a.junk = 'foo'",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT a.`id` AS ai0_5,a.`junk` AS aj1_6 FROM `A` AS a, `NB` AS b WHERE a.`id` = b.`id` and a.`junk` = 'foo'",
						null).withFakeKey(firstContainer)
					));
	}
	
	
	@Test
	public void specCont_Mixed() throws Throwable {
		// should have redist - thus no dist key
		SchemaContext db = buildBasicSchema("using container testcont (1, 'one')");
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"/* specCont_Mixed */select a.* from A a, NR b where a.id = b.id and a.junk = 'foo'",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`id` AS ai0_3,a.`junk` AS aj1_4 FROM `A` AS a WHERE a.`junk` = 'foo'",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_3" }),
								new ProjectingExpectedStep(
								"SELECT DISTINCT temp1.ai0_3 AS t3a0 FROM temp1",
									TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
								new ProjectingExpectedStep(
								"SELECT b.`id` AS bi0_4 FROM `NR` AS b, temp2 WHERE b.`id` = temp2.t3a0",
									group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"bi0_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.ai0_3 AS t5a0_7,temp1.aj1_4 AS t5a1_8 FROM temp1, temp3 WHERE temp1.ai0_3 = temp3.bi0_4",
							null)
					));
	}
	
	@Test
	public void globalCont_MixedBCast() throws Throwable {
		// no dist key set, but no redist
		SchemaContext db = buildBasicSchema();
		stmtTest(db,
				"/* globalCont_MixedBCast */select a.* from A a, NB b where a.id = b.id and a.junk = 'foo'",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT a.`id` AS ai0_5,a.`junk` AS aj1_6 FROM `A` AS a, `NB` AS b WHERE a.`id` = b.`id` and a.`junk` = 'foo'",
							null).withFakeKey(NULL_FAKE_KEY)
					));
		
	}
	
	@Test
	public void globalCont_Mixed() throws Throwable {
		// no dist key set, with redist
		SchemaContext db = buildBasicSchema();
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"/* globalCont_Mixed */select a.* from A a, NR b where a.id = b.id and a.junk = 'foo'",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`id` AS ai0_3,a.`junk` AS aj1_4 FROM `A` AS a WHERE a.`junk` = 'foo'",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_3" }),
								new ProjectingExpectedStep(
								"SELECT DISTINCT temp1.ai0_3 AS t3a0 FROM temp1",
									TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
								new ProjectingExpectedStep(
								"SELECT b.`id` AS bi0_4 FROM `NR` AS b, temp2 WHERE b.`id` = temp2.t3a0",
									group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"bi0_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.ai0_3 AS t5a0_7,temp1.aj1_4 AS t5a1_8 FROM temp1, temp3 WHERE temp1.ai0_3 = temp3.bi0_4",
							null)
					));
	}
	
	@Test
	public void nullCont_ContOnly() throws Throwable {
		SchemaContext db = buildBasicSchema("using container testcont (null)");
		assertNull(db.getCurrentTenant().get(db));
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"/* nullCont_ContOnly */select a.* from A a, B b where a.id = b.id and a.junk = 'foo'",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`id` AS ai0_3,a.`junk` AS aj1_4 FROM `A` AS a WHERE a.`junk` = 'foo'",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_3" }),
								new ProjectingExpectedStep(
								"SELECT DISTINCT temp1.ai0_3 AS t3a0 FROM temp1",
									TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
								new ProjectingExpectedStep(
								"SELECT b.`id` AS bi0_4 FROM `B` AS b, temp2 WHERE b.`id` = temp2.t3a0",
									group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"bi0_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.ai0_3 AS t5a0_7,temp1.aj1_4 AS t5a1_8 FROM temp1, temp3 WHERE temp1.ai0_3 = temp3.bi0_4",
							null)
					));
	}
	
	@Test
	public void nullCont_Mixed() throws Throwable {
		SchemaContext db = buildBasicSchema("using container testcont (null)");
		assertNull(db.getCurrentTenant().get(db));
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"/* nullCont_Mixed */select a.* from A a, NR b where a.id = b.id and a.junk = 'foo'",
				SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(
								"SELECT a.`id` AS ai0_3,a.`junk` AS aj1_4 FROM `A` AS a WHERE a.`junk` = 'foo'",
									group,"temp1",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_3" }),
								new ProjectingExpectedStep(
								"SELECT DISTINCT temp1.ai0_3 AS t3a0 FROM temp1",
									TransientExecutionEngine.LARGE,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
								new ProjectingExpectedStep(
								"SELECT b.`id` AS bi0_4 FROM `NR` AS b, temp2 WHERE b.`id` = temp2.t3a0",
									group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"bi0_4" })
							)
						),
						new ProjectingExpectedStep(
						"SELECT temp1.ai0_3 AS t5a0_7,temp1.aj1_4 AS t5a1_8 FROM temp1, temp3 WHERE temp1.ai0_3 = temp3.bi0_4",
							null)
					));
	}
	
	@Test
	public void basicTest() throws Throwable {
		SchemaContext db = buildBasicSchema("using container testcont (1, 'one')");
		stmtTest(db,
				"select b.junk, a.id from bt b, A a where b.id = a.id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT b.`junk` AS bj1,a.`id` AS ai0",
						  "FROM `bt` AS b, `A` AS a",
						  "WHERE b.`id` = a.`id`"
						).withFakeKey(firstContainer)
					));
	}	
	
	@Test
	public void testA() throws Throwable {
		SchemaContext db = buildBasicSchema("using container testcont (2, 'two')");
		stmtTest(db,
				"select b.junk from A a, NB b where a.junk = b.junk and a.id = 15",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 15",
							null).withFakeKey(secondContainer)));
	}
	
	@Test
	public void testCacheGlobal() throws Throwable {
		SchemaContext db = buildBasicSchema();
		String sqla = "select b.junk from A a, NB b where a.junk = b.junk and a.id = 2"; 
		cachePlanTest(db,
				sqla,
				false,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 2",
							null).withFakeKey(NULL_FAKE_KEY)
					));
		cachePlanTest(db,
				sqla,
				true,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 2",
							null).withFakeKey(NULL_FAKE_KEY)
					));
		// but a different string shape doesn't hit
		cachePlanTest(db,
				"select b.junk from A a, NB b where a.id = b.id and b.junk = 'foo'",
				false,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`id` = b.`id` and b.`junk` = 'foo'",
							null).withFakeKey(NULL_FAKE_KEY)
					));
		// if I switch to a particular tenant, I also don't get a hit
		TransientExecutionEngine tee = (TransientExecutionEngine) db.getCatalog();
		tee.parse(new String[] { "using container testcont (1, 'one')" });
		cachePlanTest(db,
				sqla,
				false,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 2",
							null).withFakeKey(firstContainer)
					));
//		SchemaSource ss = db.getSource();
//		SchemaCache sc = (SchemaCache) ss;
//		System.out.println(sc.describePlanCache());
	}
	
	@Test
	public void testCacheSpecific() throws Throwable {
		SchemaContext db = buildBasicSchema("using container testcont (2, 'two')");
		String sqla = "select b.junk from A a, NB b where a.junk = b.junk and a.id = 2";
		cachePlanTest(db,
				sqla,
				false,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 2",
							null).withFakeKey(secondContainer)
					));
		cachePlanTest(db,
				sqla,
				true,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 2",
							null).withFakeKey(secondContainer)
					));
		TransientExecutionEngine tee = (TransientExecutionEngine) db.getCatalog();
		tee.parse(new String[] { "using container testcont (1, 'one')" });
		cachePlanTest(db,
				sqla,
				true,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 2",
							null).withFakeKey(firstContainer)
					));
		// but if I switch to global, it should not work
		tee.parse(new String[] { "using container testcont (global)" });
		cachePlanTest(db, 
				sqla,
				false,
				bes(
						new ProjectingExpectedStep(
						"SELECT b.`junk` AS bj1_4 FROM `A` AS a, `NB` AS b WHERE a.`junk` = b.`junk` and a.`id` = 2",
							null).withFakeKey(NULL_FAKE_KEY)
					));
//		SchemaSource ss = db.getSource();
//		SchemaCache sc = (SchemaCache) ss;
//		System.out.println(sc.describePlanCache());
	}
	
}
