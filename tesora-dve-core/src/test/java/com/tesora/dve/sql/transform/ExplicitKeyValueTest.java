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




import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.execution.ConnectionValuesMap;
import com.tesora.dve.sql.transform.execution.DirectExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.execution.HasPlanning;
import com.tesora.dve.sql.util.TestName;

public class ExplicitKeyValueTest extends TransformTest {
	
	public ExplicitKeyValueTest() {
		super("ExplicitKeyValue");
	}
	
	// our tiny little schema
	private static final String[] schema = new String[] {
		"create table A (`id` integer unsigned not null, `payload` varchar(50), flags tinyint) static distribute on (`id`);",
		"create table B (`pa` integer unsigned not null, `pb` integer unsigned not null, `pc` integer unsigned not null, `payload` varchar(50)) static distribute on (`pa`, `pb`, `pc`);",
		"create table metatag_config (`id` integer unsigned not null, `instance` varchar(50), `height` varchar(50)) static distribute on (`id`);"
		};
	
	private void testOneKey(String[] schema, String sql, Map<String,Object> fakeKey, Class<?> statementClass, ExecutionType type) throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		List<Statement> stmts = parse(db, sql);
		assertEquals(stmts.size(), 1);
		Statement first = stmts.get(0);
		assertInstanceOf(first, statementClass);
		ExecutionPlan ep = Statement.getExecutionPlan(db,first); 
		List<HasPlanning> steps = ep.getSequence().getSteps();
		assertEquals(steps.size(), 1);
		DirectExecutionStep firstStep = (DirectExecutionStep) steps.get(0);
		echo(firstStep.getSQL(db,db.getValues(),"  ").resolve(db.getValues(),"  ").getDecoded());
		assertEquals(type,firstStep.getExecutionType());
		if (fakeKey != null) {
			IKeyValue kv = firstStep.getDistributionKey().getDetachedKey(db,db.getValues());
			verifyKey(fakeKey, kv);
		} else {
			assertNull(firstStep.getDistributionKey());
		}		
	}
	
	private void testOneKey(String sql, Map<String,Object> fakeKey, Class<?> statementClass, ExecutionType type) throws Throwable {
		testOneKey(schema, sql, fakeKey, statementClass, type);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void testMultiKey(String[] schema, String sql, Map[] infakes, Class<?> statementClass, ExecutionType type) throws Throwable {
		Map<String,Object>[] fakes = (Map<String,Object>[])infakes;
		HashSet<Map<String, Object>> fakeSet = new HashSet<Map<String, Object>>();
		if (fakes != null)
			fakeSet.addAll(Arrays.asList(fakes));
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		List<Statement> stmts = parse(db, sql);
		assertEquals(stmts.size(), 1);
		Statement first = stmts.get(0);
		assertInstanceOf(first, statementClass);
		ExecutionPlan ep = Statement.getExecutionPlan(db,first);
		ConnectionValuesMap cvm = new ConnectionValuesMap();
		cvm.addValues(ep, db.getValues());
		if (isNoisy())
			ep.display(db,cvm,System.out,null);
		List<HasPlanning> steps = ep.getSequence().getSteps();	
		if (fakes == null) {
			assertEquals(steps.size(), 1);
		} else {
			assertEquals(fakes.length, steps.size());
			for(HasPlanning hp : steps) {
				ExecutionStep es = (ExecutionStep) hp;
				assertEquals(es.getExecutionType(),type);
				DirectExecutionStep des = (DirectExecutionStep)es;
				DistributionKey kv = des.getDistributionKey();
				assertNotNull(kv);
				Map<String,Object> fkv = buildFakeKey(kv.getDetachedKey(db,db.getValues()));
				assertTrue(fakeSet.contains(fkv));
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void testMultiKey(String sql, Map[] infakes, Class<?> statementClass, ExecutionType type) throws Throwable {
		testMultiKey(schema, sql, infakes, statementClass, type);
	}

	// select of the form:
	// select * from A where id = 15: 
	// update A set payload = 'frob' where id = 15
	// delete from A where id = 15
	
	private void testOneKey(String tabName, String suffix, Map<String, Object> fakeKey) throws Throwable {
		testOneKey("select * from " + tabName + " " + suffix, fakeKey, SelectStatement.class, ExecutionType.SELECT);
		testOneKey("update " + tabName + " set payload = 'frob' " + suffix, fakeKey, UpdateStatement.class, ExecutionType.UPDATE);
		testOneKey("delete from " + tabName + " " + suffix, fakeKey, DeleteStatement.class, ExecutionType.DELETE);
	}
	
	@Test
	public void testSelectOneKeySimpleA() throws Throwable {
		testOneKey("A","where id = 15", buildFakeKey(new Object[] { "id", new Long(15) }));
	}

	@Test
	public void testSelectOneKeySimpleB() throws Throwable {
		testOneKey("A","where 15 = id", buildFakeKey(new Object[] { "id", new Long(15) }));
	}

	
	@Test
	public void testSelectOneKeyComplexA() throws Throwable {
		testOneKey("B","where pa = 2 and pb = 4 and pc = 6",
				buildFakeKey(new Object[] { "pa", new Long(2), "pb", new Long(4), "pc", new Long(6) }));
	}

	@Test
	public void testSelectOneKeyComplexB() throws Throwable {
		testOneKey("B","where (pa = 2 and pb = 4) and pc = 6",
				buildFakeKey(new Object[] { "pa", new Long(2), "pb", new Long(4), "pc", new Long(6) }));
	}

	@Test
	public void testSelectOneKeyComplexC() throws Throwable {
		testOneKey("B","where 2 = pa and (pb = 4 and pc = 6)",
				buildFakeKey(new Object[] { "pa", new Long(2), "pb", new Long(4), "pc", new Long(6) }));
	}

	@Test
	public void testSelectOneKeyComplexD() throws Throwable {
		testOneKey("B","where 2 = pa and (pb = 4 and pc = 6) and payload = 'foo'",
				buildFakeKey(new Object[] { "pa", new Long(2), "pb", new Long(4), "pc", new Long(6) }));
	}

	@Test
	public void testSelectOneKeyComplexE() throws Throwable {
		testOneKey("B","where 2 = pa and (pb = 4 and pc = 6) or payload = 'foo'",
				null);
	}

	@Test
	public void testSelectOneKeyComplexF() throws Throwable {
		testOneKey("B","where 2 = pa and (pb = 4 or pc = 6) and payload = 'foo'",
				null);
	}

	@SuppressWarnings("rawtypes")
	private void testMultiKey(String tab, String suffix, Map[] keys) throws Throwable {
		testMultiKey("select * from " + tab + " " + suffix, keys, SelectStatement.class, ExecutionType.SELECT);
		testMultiKey("update " + tab + " set payload = 'frob' " + suffix, keys, 
				UpdateStatement.class, ExecutionType.UPDATE);
		testMultiKey("delete from " + tab + " " + suffix, keys,
				DeleteStatement.class, ExecutionType.DELETE);
	}
	
	@Test
	public void testSelectMultiKeySimpleA() throws Throwable {
		testMultiKey("A","where id in (1,3)",
				new Map[] {
					buildFakeKey(new Object[] { "id", new Long(1) })
				});
	}

	@Test
	public void selectMultiKeySimpleB() throws Throwable {
		testMultiKey("A", "where id = 1 or id = 3",
				new Map[] {
				buildFakeKey(new Object[] { "id", new Long(1) })
				 });
	}

	@Test
	public void testSelectMultiKeySimpleC() throws Throwable {
		testMultiKey("A","where (id = 1 or id = 3) and payload = 'gobbledy'",
				new Map[] {
				buildFakeKey(new Object[] { "id", new Long(1) })
				 });
	}
	
	@Test
	public void testSelectMultiKeySimpleD() throws Throwable {
		testMultiKey("A","where (id = 1 or id = 2) or payload = 'gobbledy'",
				null);
	}

	
	@Test
	public void selectMultiKeyComplexA() throws Throwable {
		testMultiKey("B","where (pa = 1 and pb = 2 and pc = 3) or (pa = 1 and pb = 2 and pc = 5)",
				new Map[] {
				buildFakeKey(new Object[] { "pa", new Long(1), "pb", new Long(2), "pc", new Long(3) })
				});
	}
	
	@Test
	public void selectOneKeySimpleC() throws Throwable {
		testOneKey("A","where id = 15 and payload = 'whodunnit'",
				buildFakeKey(new Object[] { "id", new Long(15) }));
	}

	@Test
	public void selectOneKeySimpleD() throws Throwable {
		testOneKey("A", "where id = 15 or payload = 'whodunnit'",
				null);
	}
	
	@Test
	public void selectInSimpleA() throws Throwable {
		testOneKey("select t__0.* from A t__0 where (payload in ('global:frontpage', 'global'))", null, SelectStatement.class, ExecutionType.SELECT);
		testOneKey("select t__0.* from metatag_config t__0 where (instance in ('global:frontpage', 'global'))", null, SelectStatement.class, ExecutionType.SELECT);
	}
	
	@Test
	public void testPE282_CASE_with_IF() throws Throwable {
		String[] schema = new String[] {
			"CREATE TABLE table1 (col1 int unsigned not null default '0', col2 int unsigned not null default '0', col3 longtext, primary key (col1), key (col2));",
			"CREATE TABLE table2 (col1 int unsigned not null default '0', col2 int unsigned not null default '0', col4 int unsigned not null default '0', primary key (col4,col2), key (col1), key (col2));"		
			};
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT CASE tn.col4 ");
		sql.append("WHEN 434 THEN 'FA' ");
		sql.append("WHEN 435 THEN 'CI' ");
		sql.append("WHEN 711 THEN 'HO' ");
		sql.append("ELSE 'FA' END AS vertical, ");
		sql.append("IF(a.col3 =1, 'C','N') AS field_consignment_event_value ");
		sql.append("FROM table1 a ");
		sql.append("LEFT JOIN table2 tn ");
		sql.append("ON tn.col2=a.col2 ");
		sql.append("AND tn.col4 IN (434,435,436,711) ");
		List<Statement> stmts = parse(db, sql.toString());
		assertEquals(stmts.size(), 1);
		
		// make sure these parse out too
		stmts = parse(db, "SELECT IF(1>2,2,3)");
		assertEquals(stmts.size(), 1);
		stmts = parse(db, "SELECT IF(1<2,'yes','no')");
		assertEquals(stmts.size(), 1);
		stmts = parse(db, "SELECT IF(STRCMP('test','test1'),'no','yes')");
		assertEquals(stmts.size(), 1);
	}
	
}
