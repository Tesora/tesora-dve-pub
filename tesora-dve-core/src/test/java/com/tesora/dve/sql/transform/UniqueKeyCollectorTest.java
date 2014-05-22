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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.UniqueKeyCollector;
import com.tesora.dve.sql.transform.KeyCollector.AndedParts;
import com.tesora.dve.sql.transform.KeyCollector.EqualityPart;
import com.tesora.dve.sql.transform.KeyCollector.OredParts;
import com.tesora.dve.sql.transform.KeyCollector.Part;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.TestName;

public class UniqueKeyCollectorTest extends TransformTest {

	public UniqueKeyCollectorTest() {
		super("UniqueKeyCollectorTest");
	}

	private static final String[] schema = new String[] {
		"create table SA (`id` integer not null, `blight` int, primary key (`id`))",
		"create table CA (`fid` integer not null, `sid` integer not null, `blight` int, primary key(`fid`,`sid`))"		
	};

	private void collectKeyPart(SchemaContext sc, Map<String, Object> into, EqualityPart ep) {
		into.put(ep.getColumn().getPEColumn().get().getName().get(), ep.getLiteral().getValue(sc));
	}
	
	private Map<String, Object> buildFakeKey(SchemaContext sc, Part p) throws Throwable {
		HashMap<String, Object> out = new HashMap<String,Object>();
		if (p instanceof OredParts)
			throw new Throwable("Cannot build fake key off of ored part");
		if (p instanceof EqualityPart) {
			collectKeyPart(sc,out, (EqualityPart)p);
		} else if (p instanceof AndedParts) {
			AndedParts ap = (AndedParts) p;
			for(Part sp : ap.getParts()) {
				collectKeyPart(sc,out, (EqualityPart)sp);
			}
		}
		return out;
	}
	
	@SuppressWarnings("unused")
	private void test(TestName tn,String q) throws Throwable {
		test(tn,q,null,(Map[])null);
	}
	
	@SuppressWarnings("rawtypes")
	private void test(TestName tn, String q, String o, Map ...keys) throws Throwable { // NOPMD by doug on 04/12/12 1:53 PM
		test(tn,q,o,Collections.emptyList(),keys);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void test(TestName tn, String q, String o, List<Object> params, Map ...keys) throws Throwable {
		SchemaContext db = buildSchema(tn,schema);
		List<Statement> stmts = parse(db, q, params);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		UniqueKeyCollector ukc = new UniqueKeyCollector(db,ss.getWhereClause());
		ListSet<Part> pahts = ukc.getParts();
		if (keys == null) {
			System.out.println(q);
			if (pahts == null) System.out.println("No parts.");
			else {
				for(Part p : pahts) {
					System.out.println(p.getParent());
				}
			}			
		} else if (keys.length == 0) {
			// no keys, make sure we don't get any
			assertNull(pahts);
		} else {
			HashSet<Map<String,Object>> given = new HashSet<Map<String,Object>>();
			for(Map hm : keys)
				given.add(hm);
			if (keys.length > 1) { 
				assertTrue(pahts.get(0) instanceof OredParts);
				for(Part p : pahts.get(0).getParts()) {
					Map<String,Object> hm = buildFakeKey(db,p);
					assertTrue(given.contains(hm));
				}
			} else {
				assertEquals(pahts.size(), keys.length);
				for(Part p : pahts) {
					Map<String,Object> hm = buildFakeKey(db,p);
					assertTrue("should have key " + hm, given.contains(hm));
				}
			}
		}
		if (o != null) {
			assertEquals(o.trim(),ss.getSQL(db, false, true).trim());
		} else 
			System.out.println(ss.getSQL(db));
	}
	
	@Test
	public void testSimple() throws Throwable {
		test(TestName.MULTI,"select * from SA sa where sa.id = 1",
				"SELECT * FROM `SA` AS sa WHERE sa.`id` = 1",
				buildFakeKey(new Object[] { "id", new Long(1) }));
	}
	
	@Test
	public void testSimpleP() throws Throwable {
		test(TestName.MULTI,"select * from SA sa where sa.id = ?",
				"SELECT * FROM `SA` AS sa WHERE sa.`id` = ?",
				Collections.singletonList((Object)new Long(1)),
				buildFakeKey(new Object[] { "id", new Long(1) }));
		
	}
	
	@Test
	public void testManySimpleA() throws Throwable {
		test(TestName.MULTI,"select * from SA sa where sa.id = 1 or sa.id = 2 or sa.id = 3",
				"SELECT * FROM `SA` AS sa WHERE  (sa.`id` = 1 or sa.`id` = 2 or sa.`id` = 3)",
				buildFakeKey(new Object[] { "id", new Long(1)}),
				buildFakeKey(new Object[] { "id", new Long(2)}),
				buildFakeKey(new Object[] { "id", new Long(3)}));
	}
	
	@Test
	public void testManySimpleB() throws Throwable {
		test(TestName.MULTI,"select * from SA sa where sa.id in (1,2,3)",
				"SELECT * FROM `SA` AS sa WHERE  (sa.`id` = 1 OR sa.`id` = 2 OR sa.`id` = 3)",
				buildFakeKey(new Object[] { "id", new Long(1)}),
				buildFakeKey(new Object[] { "id", new Long(2)}),
				buildFakeKey(new Object[] { "id", new Long(3)}));
	}
	
	@Test
	public void testSinglePerverseC() throws Throwable {
		test(TestName.MULTI,"select * from SA sa where sa.id in (1)",
				"SELECT * FROM `SA` AS sa WHERE sa.`id` = 1",
				buildFakeKey(new Object[] { "id", new Long(1)}));
	}
	
	@Test
	public void testComplex() throws Throwable {
		test(TestName.MULTI,"select * from CA ca where ca.fid = 1 and ca.sid = 1",
				"SELECT * FROM `CA` AS ca WHERE  (ca.`fid` = 1 AND ca.`sid` = 1)",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(1)}));
	}
	
	@Test
	public void testComplexA() throws Throwable {
		test(TestName.MULTI,"select * from CA ca where (ca.fid = 1 and ca.sid = 1) or (ca.fid = 2 and ca.sid = 2)",
				"SELECT * FROM `CA` AS ca WHERE  ( (ca.`fid` = 1 AND ca.`sid` = 1)  or  (ca.`fid` = 2 AND ca.`sid` = 2) )",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(1)}),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(2)}));
	}
	
	@Test
	public void testComplexB() throws Throwable {
		test(TestName.MULTI,"select * from CA ca where ca.fid in (1,2,3) and ca.sid = 2",
				"SELECT * FROM `CA` AS ca WHERE  ( (ca.`fid` = 1 AND ca.`sid` = 2)  OR  (ca.`fid` = 2 AND ca.`sid` = 2)  OR  (ca.`fid` = 3 AND ca.`sid` = 2) )",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(2) }),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(2) }),
				buildFakeKey(new Object[] { "fid", new Long(3), "sid", new Long(2) }));				
	}
	
	@Test
	public void testComplexC() throws Throwable {
		test(TestName.MULTI,"select * from CA ca where ca.fid in (1,2) and ca.sid in (2,4,6)",
				"SELECT * FROM `CA` AS ca WHERE  ( (ca.`fid` = 1 AND ca.`sid` = 2)  OR  (ca.`fid` = 1 AND ca.`sid` = 4)  OR  (ca.`fid` = 1 AND ca.`sid` = 6)  OR  (ca.`fid` = 2 AND ca.`sid` = 2)  OR  (ca.`fid` = 2 AND ca.`sid` = 4)  OR  (ca.`fid` = 2 AND ca.`sid` = 6) )",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(2) }),
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(4) }),
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(6) }),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(2) }),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(4) }),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(6) }));				
	}
	
	@Test
	public void testNegativeA() throws Throwable {
		test(TestName.MULTI,"select * from SA sa where sa.id in (1,2,3) or sa.blight = 15",
				"SELECT * FROM `SA` AS sa WHERE  (sa.`id` = 1 OR sa.`id` = 2 OR sa.`id` = 3)  or sa.`blight` = 15",
				new Map[] {});
	}
	
	@Test
	public void testNegativeB() throws Throwable {
		test(TestName.MULTI,"select * from SA sa where sa.id = 1 or (sa.id = 2 and (sa.blight = 4 or sa.blight = 6))",
				"SELECT * FROM `SA` AS sa WHERE sa.`id` = 1 or  (sa.`id` = 2 and  (sa.`blight` = 4 or sa.`blight` = 6) )",
				new Map[] {});
	}
	
	@Test
	public void testSimpleMT() throws Throwable {
		test(TestName.MULTIMT,"select * from SA sa where sa.id = 1",
				"SELECT * FROM `SA` AS sa WHERE  (sa.id = 1 AND sa.`___mtid` = 42)",
				buildFakeKey(new Object[] { "id", new Long(1), "___mtid", new Long(42) }));
	}

	@Test
	public void testManySimpleAMT() throws Throwable {
		test(TestName.MULTIMT, "select * from SA sa where sa.id = 1 or sa.id = 2 or sa.id = 3",
				"SELECT * FROM `SA` AS sa WHERE  ( (sa.id = 1 AND sa.`___mtid` = 42)  or  (sa.id = 2 AND sa.`___mtid` = 42)  or  (sa.id = 3 AND sa.`___mtid` = 42) )",
				buildFakeKey(new Object[] { "id", new Long(1), "___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "id", new Long(2), "___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "id", new Long(3), "___mtid", new Long(42)}));
	}
	
	@Test
	public void testComplexMT() throws Throwable {
		test(TestName.MULTIMT,"select * from CA ca where ca.fid = 1 and ca.sid = 1",
				"SELECT * FROM `CA` AS ca WHERE  (ca.fid = 1 AND ca.sid = 1 AND ca.`___mtid` = 42)",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(1), "___mtid", new Long(42)}));
	}
	
	@Test
	public void testComplexAMT() throws Throwable {
		test(TestName.MULTIMT,"select * from CA ca where (ca.fid = 1 and ca.sid = 1) or (ca.fid = 2 and ca.sid = 2)",
				"SELECT * FROM `CA` AS ca WHERE  ( (ca.fid = 1 AND ca.sid = 1 AND ca.`___mtid` = 42)  or  (ca.fid = 2 AND ca.sid = 2 AND ca.`___mtid` = 42) )",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(1), "___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(2), "___mtid", new Long(42)}));
	}
	
	@Test
	public void testComplexBMT() throws Throwable {
		test(TestName.MULTIMT,"select * from CA ca where ca.fid in (1,2,3) and ca.sid = 2",
				"SELECT * FROM `CA` AS ca WHERE  ( (ca.fid = 1 AND ca.sid = 2 AND ca.`___mtid` = 42)  OR  (ca.fid = 2 AND ca.sid = 2 AND ca.`___mtid` = 42)  OR  (ca.fid = 3 AND ca.sid = 2 AND ca.`___mtid` = 42) )",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(2) ,"___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(2) ,"___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(3), "sid", new Long(2) ,"___mtid", new Long(42)}));				
	}
	
	@Test
	public void testComplexCMT() throws Throwable {
		test(TestName.MULTIMT,"select * from CA ca where ca.fid in (1,2) and ca.sid in (2,4,6)",
				"SELECT * FROM `CA` AS ca WHERE  ( (ca.fid = 1 AND ca.sid = 2 AND ca.`___mtid` = 42)  OR  (ca.fid = 1 AND ca.sid = 4 AND ca.`___mtid` = 42)  OR  (ca.fid = 1 AND ca.sid = 6 AND ca.`___mtid` = 42)  OR  (ca.fid = 2 AND ca.sid = 2 AND ca.`___mtid` = 42)  OR  (ca.fid = 2 AND ca.sid = 4 AND ca.`___mtid` = 42)  OR  (ca.fid = 2 AND ca.sid = 6 AND ca.`___mtid` = 42) )",
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(2) ,"___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(4) ,"___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(1), "sid", new Long(6) ,"___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(2) ,"___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(4) ,"___mtid", new Long(42)}),
				buildFakeKey(new Object[] { "fid", new Long(2), "sid", new Long(6) ,"___mtid", new Long(42)}));				
	}
}
