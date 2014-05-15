// OS_STATUS: public
package com.tesora.dve.sql.transform;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.joinsimplification.JoinSimplificationTransformFactory;
import com.tesora.dve.sql.transform.strategy.joinsimplification.NullRejectionSimplifier;
import com.tesora.dve.sql.util.TestName;

public class NullRejectionTransformTest extends TransformTest {

	public NullRejectionTransformTest() {
		super("NullRejectionTransformTest");
	}
	
	private static final String[] schema = new String[] {
		"create table A (aid int, afid int, asid int) static distribute on (aid)",
		"create table B (bid int, bfid int, bsid int) static distribute on (bid)",
		"create table C (cid int, cfid int, csid int) static distribute on (cid)"		
	};
	
	private void testRejection(String[] schema, String in, String out) throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,schema);
		SelectStatement ss = (SelectStatement) parse(db,in).get(0);
		NullRejectionSimplifier simplifier = new NullRejectionSimplifier();
		simplifier.simplify(db,ss, new JoinSimplificationTransformFactory());
		if (out == null)
			System.out.println(ss.getSQL(db));
		else
			assertEquals(out,ss.getSQL(db));
	}
	
	@Test
	public void testA() throws Throwable {
		testRejection(schema,
				"select a.afid, b.bfid from A a left outer join B b on a.aid = b.bid where b.bsid > 0",
				"SELECT a.`afid`,b.`bfid` FROM `A` AS a INNER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE b.`bsid` > 0");
	}
	
	@Test
	public void testB() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid where a.asid = 15 or b.bsid > 0",
				"SELECT a.`afid` FROM `A` AS a LEFT OUTER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE a.`asid` = 15 or b.`bsid` > 0");
	}
	
	@Test
	public void testC() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid where a.asid = 15 and b.bsid > 0",
				"SELECT a.`afid` FROM `A` AS a INNER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE a.`asid` = 15 and b.`bsid` > 0");
	}

	@Test
	public void testD() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid left outer join C c on b.bid = c.cid where c.csid > 0",
				"SELECT a.`afid` FROM `A` AS a INNER JOIN `B` AS b ON a.`aid` = b.`bid` INNER JOIN `C` AS c ON b.`bid` = c.`cid` WHERE c.`csid` > 0");
		
	}

	@Test
	public void testE() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid left outer join C c on b.bid = c.cid where b.bsid > 0",
				"SELECT a.`afid` FROM `A` AS a INNER JOIN `B` AS b ON a.`aid` = b.`bid` LEFT OUTER JOIN `C` AS c ON b.`bid` = c.`cid` WHERE b.`bsid` > 0");
	}	
	
	@Test
	public void testF() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid where b.bsid is null",
				"SELECT a.`afid` FROM `A` AS a LEFT OUTER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE b.`bsid` is NULL");
	}
	
	@Test
	public void testG() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid where b.bsid > 0 and 1=1",
				"SELECT a.`afid` FROM `A` AS a INNER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE b.`bsid` > 0");
	}

	@Test
	public void testH() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid "
				+"where b.bsid = 1 or (b.bfid = 0 and 0 <> 0 and 0 = 1) or 0 = 1",
				"SELECT a.`afid` FROM `A` AS a INNER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE b.`bsid` = 1");
	}

	@Test
	public void testI() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid where b.bsid > 0 or 1=1",
				"SELECT a.`afid` FROM `A` AS a LEFT OUTER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE true");
	}

	@Test
	public void testJ() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid where b.bsid > 0 and 1=0",
				"SELECT a.`afid` FROM `A` AS a LEFT OUTER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE false");
	}

	@Test
	public void testK() throws Throwable {
		testRejection(schema,
				"select a.afid from A a left outer join B b on a.aid = b.bid where b.bsid > 0 or 1=0",
				"SELECT a.`afid` FROM `A` AS a INNER JOIN `B` AS b ON a.`aid` = b.`bid` WHERE b.`bsid` > 0");
	}


}
