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

import org.antlr.runtime.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.standalone.PETest;


public class LexerTest {

	@BeforeClass
	public static void setup() throws Exception {
		TestHost.startServicesTransient(PETest.class);
	}

	@AfterClass
	public static void teardown() throws Exception {
		TestHost.stopServices();
	}

	static final boolean noisy = Boolean.valueOf(System.getProperty("parser.debug")).booleanValue();
	
	public static boolean isNoisy() {
		return noisy;
	}
	
	public static void echo(String what) {
		if (noisy)
			System.out.println(what);
	}

	private void one(String line, int...tokens) throws Throwable {
		Utils utils = new Utils(ParserOptions.TEST);
		sql2003Lexer lex = InvokeParser.buildLexer(InvokeParser.buildInputState(line,null), utils);
		ListOfPairs<Integer,String> actuals = new ListOfPairs<Integer,String>();
		Token t = null;
		do {
			t = lex.nextToken();
			if (t != null && t.getType() != TokenTypes.EOF)
				actuals.add(t.getType(),t.getText());
		} while (t.getType() != TokenTypes.EOF);
		if (isNoisy()) {
			StringBuilder buf = new StringBuilder();
			buf.append("'").append(line).append("' => ");
			Functional.join(actuals, buf, ", ", new BinaryProcedure<Pair<Integer,String>,StringBuilder>() {

				@Override
				public void execute(Pair<Integer, String> aobj,
						StringBuilder bobj) {
					bobj.append(TokenTypes.tokenNames[aobj.getFirst()]).append(":'").append(aobj.getSecond()).append("'");
				}
				
			});
			echo(buf.toString());
		}
		if (tokens != null) {
			assertEquals("must have same number of tokens",tokens.length, actuals.size());
			for(int i = 0; i < tokens.length; i++) {
				assertEquals(i + " token must be same",tokens[i],actuals.get(i).getFirst().intValue());
			}
		}
	}
		
	@Test
	public void testNonNumeric() throws Throwable {
		one("a+b",TokenTypes.Regular_Identifier, TokenTypes.Plus_Sign, TokenTypes.Regular_Identifier);
	}
	
	@Test
	public void testFloatingPoint() throws Throwable {
		one("0.01",null);
		one("1.e5",null);
		one(".1e5",null);
		one("-1.e+5",null);
	}

	@Test
	public void testIntegral() throws Throwable {
		one("8e-05",null);
		one("1e+3",null);
		one("-1e-3",null);
		one("1 + 2",null);
		one("1 + -2.e+4",null);
		one("1+-2.e+4",null);
		one("-1 -2",null);
	}

	@Test
	public void testIdentifier() throws Throwable {
		one("`8e-05`",null);
	}
	
	@Test
	public void testBinaryLiteral() throws Throwable {
		
	}
	
}
