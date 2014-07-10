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


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.TransientSchemaTest;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.TestName;
import com.tesora.dve.standalone.PETest;

public abstract class TestTypes extends TransientSchemaTest {

	public TestTypes() {
		super("TestTypes");
	}

	@BeforeClass
	public static void setup() throws Exception {
		TestHost.startServicesTransient(PETest.class);
	}

	@AfterClass
	public static void teardown() throws Exception {
		TestHost.stopServices();
	}

	public abstract List<NativeType> getNativeTypes();
	
	@Test
	public void testTypes() throws Throwable {
		for(NativeType nt : getNativeTypes()) {
			if (nt.isUsedInCreate()) {
				// timestamp has weird rules, for instance it's not by default nullable - you have to explicitly
				// set it to be nullable
				if (nt.isTimestampType()) continue;
				TypeEntry te = new TypeEntry(nt);
				List<TypeComboMixedCase> cases = new ArrayList<TypeComboMixedCase>();
				int counter = 0;
				for(TypeComboEntry tce : te.getEntries()) {
					cases.add(new TypeComboMixedCase(tce, (++counter)));
				}
				SchemaContext pc = buildDatabase(TestName.MULTI);
				// build the create table statement
				String tableDecl = buildTableDeclaration(cases);
				PECreateStatement<?,?> cts = 
					(PECreateStatement<?,?>)(InvokeParser.parse(InvokeParser.buildInputState(tableDecl,null), ParserOptions.TEST, pc).getStatements().get(0));
				PETable t = (PETable)cts.getCreated().get();
				for(TypeComboMixedCase tcmc : cases) {
					tcmc.match(pc,t);
				}
			}
		}
	}

	public String buildTableDeclaration(List<TypeComboMixedCase> types) {
		StringBuilder buf = new StringBuilder();
		buf.append("CREATE TABLE typeTest ( ");
		ArrayList<String> decls = new ArrayList<String>();
		for(Iterator<TypeComboMixedCase> iter = types.iterator(); iter.hasNext();) {
			TypeComboMixedCase tcmc = iter.next();
			for(String d : tcmc.getDeclarations()) {
				decls.add(d);
			}
		}
		Functional.join(decls, buf, ", ");
		buf.append(" );");
		return buf.toString();
	}



	public static class TypeComboMixedCase {
		
		// they are all based on the same type combo
		private TypeComboEntry entry;
		// but there are three different cases, so one decl each
		private String[] columnNames;
		private String[] decls;
		
		@SuppressWarnings("synthetic-access")
		public TypeComboMixedCase(TypeComboEntry source, int suffix) {
			entry = source;
			columnNames = new String[] { "ctt_" + suffix, "utt_" + suffix, "mtt_" + suffix };
			decls = new String[3];
			decls[0] = "`" + columnNames[0] + "` " + entry.getDeclaration().toUpperCase();
			decls[1] = "`" + columnNames[1] + "` " + entry.getDeclaration().toLowerCase();
			decls[2] = "`" + columnNames[2] + "` " + mixedCap(entry.getDeclaration());
		}
		
		public String[] getDeclarations() {
			return decls;
		}
		
		public void matchSingle(SchemaContext pc, PETable tab, String columnName) {
			PEColumn c = tab.lookup(pc,new UnqualifiedName(columnName).getQuotedName());
			assertNotNull(c);
			String message = entry.matches(c);
			if (message != null)
				fail(message);
		}
		
		public void match(SchemaContext pc, PETable tab) {
			for(String cn : columnNames)
				matchSingle(pc, tab, cn);
		}
	}
	
	public static class TypeComboEntry {
		
		private String decl;
		private NativeType base;
		private Boolean nulled;
		private Boolean unsigned;
		private Integer scale;
		private Integer precision;
		
		public TypeComboEntry(String decl, NativeType nt, 
				Boolean nulled, Boolean unsigned, Integer scale, Integer precision) {
			this.decl = decl;
			this.base = nt;
			this.nulled = nulled;
			this.unsigned = unsigned;
			this.scale = scale;
			this.precision = precision;
		}
		
		public String getDeclaration() { return this.decl; }
		
		public TypeComboEntry makeNotNull() {
			return new TypeComboEntry(decl + " NOT NULL", base, Boolean.FALSE, this.unsigned, this.scale, this.precision);
		}
		
		public TypeComboEntry makeNullable() {
			return new TypeComboEntry(decl, base, Boolean.TRUE, this.unsigned, this.scale, this.precision);
		}
		
		public TypeComboEntry makeUnsigned() {
			return new TypeComboEntry(decl + " " + MysqlNativeType.MODIFIER_UNSIGNED, base, this.nulled,
					Boolean.TRUE, this.scale, this.precision);
		}
		
		public TypeComboEntry makeSigned() {
			return new TypeComboEntry(decl, base, this.nulled, Boolean.FALSE, this.scale, this.precision);
		}
		
		public TypeComboEntry makeSized(int prec) {
			return new TypeComboEntry(decl + "(" + prec + ")", base, this.nulled, this.unsigned, this.scale, new Integer(prec));
		}
		
		public TypeComboEntry makeScaled(int prec, int scaleValue) {
			return new TypeComboEntry(decl + "(" + prec + ", " + scaleValue + ")", base, this.nulled, this.unsigned, new Integer(scaleValue), new Integer(prec));
		}
			
		public String matches(PEColumn c) {
			if (c.getType().getBaseType() != base) 
				return "Expected base type: " + base.getTypeName() + " but found " + c.getType().getBaseType().getTypeName();
			if (nulled != null) {
				if ((nulled.booleanValue() && c.isNullable()) || (!nulled.booleanValue() && !c.isNullable())) {
					// ok
				} else {
					return "Expected to " + (nulled ? "be" : "not be") + " nullable, but found to " + (c.isNullable() ? "be" : "not be") + " nullable";
				}
			}
			return null;
		}
	}
	
	public static class TypeEntry {
		
		private NativeType backing;
		private List<TypeComboEntry> entries;
		
		public TypeEntry(NativeType nt) {
			backing = nt;
			entries = buildCombos();
		}

		public List<TypeComboEntry> getEntries() {
			return entries;
		}
		
		private List<TypeComboEntry> buildCombos() {
			boolean supportsNullable = backing.isNullable();
			boolean supportsUnsigned = backing.isUnsignedAttribute();
			boolean supportsPrecision = backing.getSupportsPrecision();
			boolean supportsScale = backing.getSupportsScale();
			ArrayList<TypeComboEntry> decls = new ArrayList<TypeComboEntry>();
			decls.add(new TypeComboEntry(backing.getTypeName(), backing, Boolean.TRUE, null, null, null));
			ArrayList<TypeComboEntry> buf = new ArrayList<TypeComboEntry>();
			if (supportsPrecision) {
				buf.clear();
				for(TypeComboEntry tce : decls) {
					buf.add(tce.makeSized(25));
					if (supportsScale)
						buf.add(tce.makeScaled(25, 2));
				}
				decls.addAll(buf);
			}
			if (supportsUnsigned) {
				buf.clear();
				for(TypeComboEntry tce : decls) {
					buf.add(tce.makeUnsigned());
					buf.add(tce.makeSigned());
				}
				decls.addAll(buf);
			}
			if (supportsNullable) {
				buf.clear();
				for(TypeComboEntry tce : decls) {
					buf.add(tce.makeNotNull());
					buf.add(tce.makeNullable());
				}
				decls.addAll(buf);
			}
			return decls;
		}
	}
	
	
		
	private static String mixedCap(String in) {
		StringBuilder buf = new StringBuilder();
		for(int i = 0; i < in.length(); i++) {
			char c = in.charAt(i);
			if (Character.isLetter(c)) {
				boolean cap = ((i % 2) == 0);
				if (cap) {
					buf.append(Character.toUpperCase(c));
				} else {
					buf.append(Character.toLowerCase(c));
				}
			} else {
				buf.append(c);
			}
		}
		return buf.toString();
	}	
}
