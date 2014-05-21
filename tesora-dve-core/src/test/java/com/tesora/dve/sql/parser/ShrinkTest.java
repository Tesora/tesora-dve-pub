// OS_STATUS: public
package com.tesora.dve.sql.parser;

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
import static org.junit.Assert.assertFalse;

import java.io.BufferedReader;
import java.io.FileReader;

import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.util.Pair;

public class ShrinkTest {

	private void testOne(String in,	ExtractedLiteral[] literals, String out) {
		CandidateParser cp = new CandidateParser(in);
		boolean success = cp.shrink();
		if (literals == null && out == null) {
			assertFalse("not expected to be shrunk",success);
			return;
		} else {
			assertEquals("expect correct shrunk",out, cp.getShrunk());
			if (literals == null)
				assertEquals("expect no literals",0,cp.getLiterals().size());
			else {
				assertEquals("expect same number of literals",literals.length,cp.getLiterals().size());
				for(int i = 0; i < literals.length; i++) 
					assertEquals("expect same value",literals[i],cp.getLiterals().get(i));
			}
		}
	}
	
	private static final String simple1 = "select * from A";
	
	@Test
	public void testSimple1() {
		testOne(simple1,null,simple1);
	}
	
	private static final String simple2 = "select * from (select * from A) subquery"; 
	
	@Test
	public void testSimple2() {
		testOne(simple2,null,simple2);
	}
	
	private static final String simpleString = "select * from A where name like 'foo' and age < now()"; 
	
	@Test
	public void testSimpleString() {
		testOne(simpleString,
				new ExtractedLiteral[] { ExtractedLiteral.makeStringLiteral("'foo'",32) },
				"select * from A where name like ? and age < now()");
	}

	private static Pair<String,String> makeEscapedStringTest() {
		StringBuffer flabby = new StringBuffer();
		flabby.append("'").append("fla").append("\\").append("'").append("bbergasted").append("'");
		String flab = flabby.toString();
		return new Pair<String,String>("select * from A where name = " + flab + " and age < now()", flab);
	}
	
	@Test
	public void testSimpleStringEscape() {
		Pair<String,String> in = makeEscapedStringTest();
		testOne(in.getFirst(),
				new ExtractedLiteral[] { ExtractedLiteral.makeStringLiteral(in.getSecond(),29) },
				"select * from A where name = ? and age < now()");
	}

	private static final String simpleIntLiteral = "select * from A where id = 1 and age < now()";
	
	@Test
	public void testSimpleIntLiteral() {
		testOne(simpleIntLiteral,
				new ExtractedLiteral[] { ExtractedLiteral.makeIntegralLiteral("1",27) },
				"select * from A where id = ? and age < now()");
	}
	
	private static final String simpleDecLiteral = "select * from A where id < 1.1 and age < now()"; 
	
	@Test
	public void testSimpleDecLiteral() {
		testOne(simpleDecLiteral,
				new ExtractedLiteral[] { ExtractedLiteral.makeDecimalLiteral("1.1",27) },
				"select * from A where id < ? and age < now()");
	}
	
	private static final String complexIntLiteral = "update A set id = id+1 where (2=3)"; 
	
	@Test
	public void testComplexIntLiteral() {
		testOne(complexIntLiteral,
				new ExtractedLiteral[] { ExtractedLiteral.makeIntegralLiteral("1",21),
					ExtractedLiteral.makeIntegralLiteral("2",30),
					ExtractedLiteral.makeIntegralLiteral("3",32) },
				"update A set id = id+? where (?=?)");
	}

	private static final String literalAtEnd = "select * from A where id = 15"; 
	
	@Test
	public void testLiteralAtEnd() {
		testOne(literalAtEnd,
				new ExtractedLiteral[] { ExtractedLiteral.makeIntegralLiteral("15",27) },
				"select * from A where id = ?");
	}

	private static final String binaryLiteral = "select * from A where id = B'001001'";
	
	@Test
	public void testBinaryLiteral() {
		testOne(binaryLiteral,
				null, null);
	}
	
	private static final String hexLiteral = "select * from A where id = x'0579AF' or id = x'123456'";
	
	@Test
	public void testHexLiteral() {
		testOne(hexLiteral,
				new ExtractedLiteral[] { ExtractedLiteral.makeHexLiteral("x'0579AF'",27),
					ExtractedLiteral.makeHexLiteral("x'123456'",37) },
				"select * from A where id = ? or id = ?");
	}
	
	private static final String singleCharLiterals = "select * from testa where payload in ('a','b')"; 
	
	@Test
	public void testBadHexLiteral() {
		testOne("select * from A where id = x'12345 and id = x'123456'",null,null);
	}
	
	@Test
	public void testSingleCharacterLiterals() {
		testOne(singleCharLiterals,
				new ExtractedLiteral[] { ExtractedLiteral.makeStringLiteral("'a'",38),
					ExtractedLiteral.makeStringLiteral("'b'",40)},
				"select * from testa where payload in (?,?)");
	}
	
	private static final String acquiaA = "select redirect.rid as rid from redirect redirect where (( source like 'node' escape "
		+ "'" + '\\' + '\\' + "') or (source = '') ) and (language in ('en', 'und'))"; 
	
	@Test
	public void testAcquiaA() {
		testOne(acquiaA,
				new ExtractedLiteral[] {
					ExtractedLiteral.makeStringLiteral("'node'",71),
					ExtractedLiteral.makeStringLiteral("'\\\\'",80),
					ExtractedLiteral.makeStringLiteral("''",96),
					ExtractedLiteral.makeStringLiteral("'en'",119),
					ExtractedLiteral.makeStringLiteral("'und'",122)	},
				"select redirect.rid as rid from redirect redirect where (( source like ? escape ?) or (source = ?) ) and (language in (?, ?))");
	}
	
	private static final String acquiaB = "select t__0.* from file_display t__0 where (name in ('what','a','mess'))"; 
	
	@Test
	public void testAcquiaB() {
		testOne(acquiaB,
				new ExtractedLiteral[] { ExtractedLiteral.makeStringLiteral("'what'",53),
					ExtractedLiteral.makeStringLiteral("'a'",55),
					ExtractedLiteral.makeStringLiteral("'mess'",57) },
				"select t__0.* from file_display t__0 where (name in (?,?,?))");		
	}

	private static final String sbtest16 = "UPDATE sbtest15 SET k=k+1 WHERE id=42445";
	@Test
	public void testSysbenchTest16() {
		testOne(sbtest16,
				new ExtractedLiteral[] { 
					ExtractedLiteral.makeIntegralLiteral("1",24), 
					ExtractedLiteral.makeIntegralLiteral("42445",35) 
				},
				"UPDATE sbtest15 SET k=k+? WHERE id=?");
	}
	
	
	private static final String analyzerTestNG44 = 
			"SELECT COUNT(a.uid) FROM users a INNER JOIN bs_user b on b.uid = a.uid INNER JOIN uc_countries x ON b.country = x.country_name WHERE a.status=1 and b.activation_status=1 AND (b.activation_date BETWEEN '10' AND '2')";
	@Test
	public void testAnalyzerTestLine44() {
		testOne(analyzerTestNG44,
				new ExtractedLiteral[] {
				ExtractedLiteral.makeIntegralLiteral("1",142),
				ExtractedLiteral.makeIntegralLiteral("1",168),
				ExtractedLiteral.makeStringLiteral("'10'",201),
				ExtractedLiteral.makeStringLiteral("'2'",207) },
				"SELECT COUNT(a.uid) FROM users a INNER JOIN bs_user b on b.uid = a.uid INNER JOIN uc_countries x ON b.country = x.country_name WHERE a.status=? and b.activation_status=? AND (b.activation_date BETWEEN ? AND ?)"
				);
	}
		
	private static final String pe1408A = 
			"select" + PEConstants.LINE_SEPARATOR + "a.id,  " + '\t' + "b.id   from A   a join B b on a.foo" + PEConstants.LINE_SEPARATOR + "=b.foo";

	@Test
	public void testPE1408() {
		testOne(pe1408A,
				new ExtractedLiteral[] {},
				"select a.id, b.id from A a join B b on a.foo =b.foo");
	}
	
	
	private static final String[] queries = new String[] {
		binaryLiteral, complexIntLiteral, literalAtEnd, simple1, simple2, simpleDecLiteral,simpleIntLiteral,
		simpleString, singleCharLiterals, makeEscapedStringTest().getFirst(), hexLiteral, sbtest16, analyzerTestNG44,
		pe1408A
	};

	@Test
	public void perfAll() {
		for(int i = 0; i < 100000; i++) {
			for(int j = 0; j < queries.length; j++) {
				CandidateParser cp = new CandidateParser(queries[j]);
				cp.shrink();
			}
		}		
	}
	
	public static final void main(String[] in) {
		if (in.length < 1) {
			System.out.println("Usage <filename> -w");
			return;
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader(in[0]));
			boolean warn = in.length > 1;
			String line = null;
			while((line = br.readLine()) != null) {
				CandidateParser cp = new CandidateParser(line);
				if (!cp.shrink() && warn) {
					System.out.println("Failed: '" + line + "'");
				}
			}
			br.close();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
}
