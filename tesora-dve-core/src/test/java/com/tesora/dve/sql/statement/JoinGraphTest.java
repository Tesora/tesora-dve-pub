package com.tesora.dve.sql.statement;

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

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.TestName;

public class JoinGraphTest extends TransientSchemaTest {

	public JoinGraphTest() {
		super("JoinGraphTest");
	}

	private static final String body = "(id int, fid int, sid int, primary key (id))";
	private static final String[] schema = {
		"create table AA " + body,
		"create table AB " + body,
		"create table BA " + body + " broadcast distribute",
		"create table BB " + body + " broadcast distribute",
		"create table RA " + body + " range distribute on (id) using openrange",
		"create table RB " + body + " range distribute on (id) using openrange",
		"create table RC " + body + " range distribute on (fid) using openrange",
		"create table RD " + body + " range distribute on (fid) using openrange",
		"create table SA " + body + " static distribute on (id)",
		"create table SB " + body + " static distribute on (id)",
		"create table SC " + body + " static distribute on (fid)",
		"create table SD " + body + " static distribute on (fid)"
	};

	
	@Test
	public void testIJChain() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);		
		joinGraphTest(db,"select aa.id from AA aa inner join BA ba on aa.sid = ba.sid",1,0);
		joinGraphTest(db,"select aa.id from AA aa inner join AB ab on aa.sid = ab.sid",2,1);
	}
	
	@Test
	public void testIJ3Way() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		joinGraphTest(db,"select aa.id from AA aa inner join AB ba on aa.sid = ba.sid inner join SA sa on sa.id = aa.id and sa.fid = ba.fid",
				3,3);
		joinGraphTest(db,"select aa.id from AA aa inner join BA ba on aa.sid = ba.sid inner join SA sa on sa.id = aa.id and sa.fid = ba.fid",
				2,2);
		joinGraphTest(db,"select aa.id from AA aa inner join AB ab on aa.sid = ab.sid inner join BA ba on ba.id = aa.id and ba.fid = ab.fid",
				3,3);
		joinGraphTest(db,"select aa.id from AA aa inner join BA ba on aa.sid = ba.sid inner join BB bb on bb.id = aa.id and bb.fid = ba.fid",
				1,0);
	}

	// migrate the old partition test tests
	@Test
	public void testInformalJoins() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);
		joinGraphTest(db,"select sa.fid from SA sa, BA ba where sa.id = ba.id and sa.sid = ba.sid",1,0);
		joinGraphTest(db,"select sa.fid from SA sa, BA ba where sa.sid = ba.sid and sa.fid = 1",1,0);
//		joinGraphTest(db,"select aa.* from AA aa, BA ba, RA ra, SA sa where aa.id = ba.id and ba.fid = ra.fid and ra.sid = sa.sid",3,2);
	}
	
	@Test
	public void testColocationRules() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);
		// random is never colocated with anything except broadcast
		for(String r : new String[]{ "AB", "RA", "SA" }) 
			joinGraphTest(db,"select a.sid from AA a inner join " + r + " b on a.id=b.id where a.fid = 22",2,1);			
		joinGraphTest(db,"select a.sid from AA a inner join BA b on a.id=b.id where a.fid = 22",1,0);
		// bcast is always colocated for inner joins
		for(String r : new String[] { "AA", "BB", "RA", "SA" })
			joinGraphTest(db,"select a.sid from BA a inner join " + r + " b on a.id=b.id where a.fid = 22",1,0);
		// but never for outer joins - except for bcast rhs 
		for(String r : new String[] { "AA", "RA", "SA" })
			joinGraphTest(db,"select a.sid from BA a left outer join " + r + " b on a.id=b.id where a.fid = 22",2,1);
		// same range is colocated
		joinGraphTest(db,"select a.sid from RA a inner join RC c on a.id = c.fid",1,0);
		// likewise static
		joinGraphTest(db,"select a.sid from SA a inner join SC c on a.id = c.fid",1,0);
	}
	
	@Test
	public void testItAintTransitive_aka_PE876() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, schema);

		// this is the simplest case
		// RA ij BA ij RC - not colocated
		joinGraphTest(db,"select ra.sid from RA ra inner join BA ba on ra.id = ba.id inner join RC rc on ba.fid = rc.fid",2,1);
		// RA ij RB ij BA ij RC ij RD - {RA,RB},{BA},{RC,RD} is the basic set
		joinGraphTest(db,"select ra.sid "
				+ "from RA ra inner join RB rb on ra.id=rb.id "
				+ "left outer join BA ba on rb.sid=ba.sid "
				+ "inner join RC rc on rc.id=ba.id "
				+ "inner join RD rd on rc.fid=rd.fid",
				2,1);
		// RA ij RB ij RC ij BA - should end up with {RA,RB},{RC,BA}
		joinGraphTest(db,"select ra.sid "
				+ "from RA ra inner join RB rb on ra.id=rb.id "
				+ "inner join RC rc on rb.fid=rc.fid "
				+ "inner join BA ba on rc.id=ba.id",
				2,1);
		// AA ij BA ij AB - two parts again
		joinGraphTest(db,"select aa.sid "
				+ "from AA aa inner join BA ba on aa.id=ba.id "
				+ "inner join AB ab on ba.id=ab.id",
				2,1);
		// BA ij AA ij AB - two parts
		joinGraphTest(db,"select ba.sid "
				+ "from BA ba inner join AA aa on ba.id=aa.id "
				+ "inner join AB ab on ba.id=ab.id",
				2,1);
		joinGraphTest(db,"select ba.sid "
				+ "from BA ba inner join RA ra on ba.sid=ra.id "
				+ "inner join RB rb on ba.fid=rb.id",
				2,1);
	}
	
}
