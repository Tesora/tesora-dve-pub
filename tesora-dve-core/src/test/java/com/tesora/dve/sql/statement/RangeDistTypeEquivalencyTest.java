// OS_STATUS: public
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

import static org.junit.Assert.fail;

import org.junit.Test;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.util.TestName;

public class RangeDistTypeEquivalencyTest extends TransientSchemaTest {

	public RangeDistTypeEquivalencyTest() {
		super("RangeDistTypeEquivalencyTest");
	}
	
	private void testEquivalency(String[] rangeDecls, TypeTest...tests) throws Throwable {
		for(String rangeDistType : rangeDecls) {
			String rangeDecl = "create range eqtest (" + rangeDistType + ") persistent group g1";
			for(TypeTest tt : tests) {
				for(String cdecl : tt.getDeclarations()) {
					String tabDecl = "create table foo (`id` int, `dv` " + cdecl + ", primary key (`id`)) range distribute on (`dv`) using eqtest";
					try {
						buildSchema(TestName.MULTI, rangeDecl, tabDecl);
						if (!tt.isValid())
							fail("should have failed using a " + cdecl + " in a range declared as " + rangeDistType);
					} catch (SchemaException se) {
						if (tt.isValid())
							throw se;
						// make sure it's invalid for the right reason
						String message = se.getMessage();
						if (se.getPass() != Pass.SECOND)
							throw se;
						if (message.indexOf("cannot be used with range") == -1)
							throw se;
					}
				}
			}
		}
	}
	
	@Test
	public void testBigInt() throws Throwable {
		testEquivalency(new String[] { "bigint", "bigint unsigned", "bigint(15)" },
				new TypeTest(true,"bigint","bigint(22)","bigint unsigned"),
				new TypeTest(true,"int","integer","int unsigned","integer unsigned","int(10)","int(11)","integer(11)"),
				new TypeTest(true,"smallint","smallint unsigned","smallint(5)"),
				new TypeTest(true,"tinyint","tinyint unsigned","tinyint(5)"),
				new TypeTest(false, "timestamp", "time", "date", "datetime"),
				new TypeTest(true,"bit","bit(3)"));
	}
	
	@Test
	public void testInt() throws Throwable {
		testEquivalency(new String[] { "int", "integer", "int unsigned", "integer unsigned", "int(10)", "integer(11)" },
				new TypeTest(false,"bigint","bigint(22)","bigint unsigned"),
				new TypeTest(true,"int","integer","int unsigned","integer unsigned","int(10)","int(11)","integer(11)"),
				new TypeTest(true,"smallint","smallint unsigned","smallint(5)"),
				new TypeTest(true,"tinyint","tinyint unsigned","tinyint(5)"),
				new TypeTest(false, "timestamp", "time", "date", "datetime"),
				new TypeTest(true,"bit","bit(3)"));
	}
	
	@Test
	public void testSmallint() throws Throwable {
		testEquivalency(new String[] { "smallint", "smallint unsigned", "smallint(5)" },
				new TypeTest(false,"bigint","bigint(22)","bigint unsigned"),
				new TypeTest(false,"int","integer","int unsigned","integer unsigned","int(10)","int(11)","integer(11)"),
				new TypeTest(true,"smallint","smallint unsigned","smallint(5)"),
				new TypeTest(true,"tinyint","tinyint unsigned","tinyint(5)"),
				new TypeTest(false, "timestamp", "time", "date", "datetime"),
				new TypeTest(true,"bit","bit(3)"));
	}
	
	@Test
	public void testTinyint() throws Throwable {
		testEquivalency(new String[] { "tinyint", "tinyint unsigned", "tinyint(3)" }, 
				new TypeTest(false,"bigint","bigint(22)","bigint unsigned"),
				new TypeTest(false,"int","integer","int unsigned","integer unsigned","int(10)","int(11)","integer(11)"),
				new TypeTest(false,"smallint","smallint unsigned","smallint(5)"),
				new TypeTest(true,"tinyint","tinyint unsigned","tinyint(5)"),
				new TypeTest(false, "timestamp", "time", "date", "datetime"),
				new TypeTest(true,"bit","bit(3)"));
	}

	@Test
	public void testTimeType() throws Throwable {
		testEquivalency(new String[] { "time" },
				new TypeTest(true, "time"),
				new TypeTest(false, "timestamp", "date", "datetime"),
				new TypeTest(false, "bigint", "bigint unsigned", "bigint(20)",
						"int", "int unsigned", "int(10)",
						"integer", "integer unsigned", "integer(11)",
						"smallint", "smallint unsigned", "smallint(5)",
						"tinyint", "tinyint unsigned", "tinyint(5)",
						"bit", "bit(3)"));
	}

	@Test
	public void testDateType() throws Throwable {
		testEquivalency(new String[] { "date" },
				new TypeTest(true, "date"),
				new TypeTest(false, "timestamp", "time", "datetime"),
				new TypeTest(false, "bigint", "bigint unsigned", "bigint(20)",
						"int", "int unsigned", "int(10)",
						"integer", "integer unsigned", "integer(11)",
						"smallint", "smallint unsigned", "smallint(5)",
						"tinyint", "tinyint unsigned", "tinyint(5)",
						"bit", "bit(3)"));
	}

	@Test
	public void testDateTimeType() throws Throwable {
		testEquivalency(new String[] { "datetime" },
				new TypeTest(true, "datetime"),
				new TypeTest(false, "timestamp", "time", "date"),
				new TypeTest(false, "bigint", "bigint unsigned", "bigint(20)",
						"int", "int unsigned", "int(10)",
						"integer", "integer unsigned", "integer(11)",
						"smallint", "smallint unsigned", "smallint(5)",
						"tinyint", "tinyint unsigned", "tinyint(5)",
						"bit", "bit(3)"));
	}

	@Test
	public void testTimestampType() throws Throwable {
		testEquivalency(new String[] { "timestamp" },
				new TypeTest(true, "timestamp"),
				new TypeTest(false, "datetime", "time", "date"),
				new TypeTest(false, "bigint", "bigint unsigned", "bigint(20)",
						"int", "int unsigned", "int(10)",
						"integer", "integer unsigned", "integer(11)",
						"smallint", "smallint unsigned", "smallint(5)",
						"tinyint", "tinyint unsigned", "tinyint(5)",
						"bit", "bit(3)"));
	}
	
	@Test
	public void testVarcharType() throws Throwable {
		testEquivalency(new String[] { "varchar", "character varying" },
				new TypeTest(true, "varchar(255)", "character varying(255)"));
	}

	@Test
	public void testUnsuitableForRange() throws Throwable {
		String[] types = new String[] { 
				"float", "double", "real", "double precision", 
				"enum('a','b')",
				"decimal(10,0)", "decimal(22,5), dec(22,5), fixed(22,5)",
				"bit", "bit(5)"
		};
		for(String s : types) {
			try {
				String rangeDecl = "create range eqtest (" + s + ") persistent group g1";
				buildSchema(TestName.MULTI, rangeDecl);
				fail("should not be able to create a range of type " + s);
			} catch (SchemaException se) {
				if (se.getMessage().startsWith("Invalid type for range distribution"))
					continue;
				throw se;
			}
		}
		
	}
	
	private static class TypeTest {
		
		private String[] decl;
		private boolean works;
		
		public TypeTest(boolean v, String...decls) {
			decl = decls;
			works = v;
		}
		
		public String[] getDeclarations() {
			return decl;
		}
		
		public boolean isValid() {
			return works;
		}
	}
	
}
