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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import com.tesora.dve.sql.TransformException;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PESchema;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.transform.TransformTest;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.TestName;

public class InsertNormalizationTest extends TransformTest {

	public InsertNormalizationTest() {
		super("NormalizationTest");
	}
	
	private static final String[] nonIncrInsertSchema = new String[] {
		"create table nonincrinsert ( `id` integer not null, `payload` varchar(50), `counter` integer default 15 ) static distribute on (`id`);"
	};
	
	private static final String[] defaultsSchema = new String[] {
		"create table defs ( `id` integer not null default 0, `payload` varchar(50) default null, `counter` integer default 15);",
		"create table nodefs ( `id` integer, `payload` varchar(50));"
	};

	private SchemaContext buildNonIncrInsertSchema() throws Exception {
		return buildDatabase(TestName.MULTI,nonIncrInsertSchema);
	}
	
	private SchemaContext buildDefsSchema() throws Exception {
		return buildDatabase(TestName.MULTI,defaultsSchema);
	}
	
	
	@Test
	public void testNonIncrTableParse() throws Exception {
		SchemaContext deb = buildNonIncrInsertSchema();
		PESchema schema = deb.getCurrentPEDatabase().getSchema();
		PETable tab = schema.buildInstance(deb,new UnqualifiedName("nonincrinsert"),null).getAbstractTable().asTable();
		List<PEColumn> columns = Functional.toList(tab.getColumns(deb));
		assertTrue(columns.get(0).getName().get().equals("id"));
		assertTrue(columns.get(1).getName().get().equals("payload"));
		assertTrue(columns.get(2).getName().get().equals("counter"));
	}
	
	private InsertIntoValuesStatement parseInsert(SchemaContext db, String sql) throws Exception {
		List<Statement> stmts = parse(db, sql);
		assertTrue(stmts.size() == 1);
		InsertIntoValuesStatement is = (InsertIntoValuesStatement)stmts.get(0);
		is.normalize(db);
		db.getValueManager().handleAutoincrementValues(db);
		return is;
	}
	
	@Test
	public void noColumnsSpecified() throws Exception {
		SchemaContext db = buildNonIncrInsertSchema();
		String insert = "insert into nonincrinsert () values (1, 'whodunnit', 2);";
		InsertIntoValuesStatement is = parseInsert(db, insert);
		// columns should be in declaration order
		PETable tab = is.getTableInstance().getAbstractTable().asTable();
		assertColumnsAre(db, is.getColumnSpecification(), tab, new String[] { "id", "payload", "counter" });
		assertValuesAre(db,is.getValues().get(0), new Object[] { new Long(1), "whodunnit", new Long(2) });
	}
	
	@Test
	public void nothingSpecified() throws Exception {
		SchemaContext db = buildDefsSchema();
		String insert = "insert into defs () values ();";
		InsertIntoValuesStatement is = parseInsert(db, insert);
		PETable tab = is.getTableInstance().getAbstractTable().asTable();
		assertColumnsAre(db, is.getColumnSpecification(), tab, new String[] { "id", "payload", "counter" });
		assertValuesAre(db,is.getValues().get(0), new Object[] { "0", null, "15" });
	}
	
	@Test
	public void defaultValuesNonIncr() throws Exception {
		SchemaContext db= buildNonIncrInsertSchema();
		String insert = "insert into nonincrinsert (id, payload) values (1, 'whodunnit')";
		InsertIntoValuesStatement is = parseInsert(db, insert);
		assertColumnsAre(db, is.getColumnSpecification(), is.getTableInstance().getAbstractTable().asTable(), new String[] { "id", "payload", "counter" });
		assertValuesAre(db,is.getValues().get(0), new Object[] { new Long(1), "whodunnit", "15" });
	}
	
	@Test
	public void nullableValuesNonIncr() throws Exception {
		SchemaContext db= buildNonIncrInsertSchema();
		String insert = "insert into nonincrinsert (id, counter) values (1, 22)";
		InsertIntoValuesStatement is = parseInsert(db, insert);
		PETable tab = is.getTableInstance().getAbstractTable().asTable();
		assertColumnsAre(db, is.getColumnSpecification(), tab, new String[] { "id", "counter", "payload" });
		assertValuesAre(db,is.getValues().get(0), new Object[] { new Long(1), new Long(22), null });
	}
	
	@Test
	public void invalidValuesSize() throws Exception {
		SchemaContext db= buildNonIncrInsertSchema();
		String insert = "insert into nonincrinsert (id, counter) values (1)";
		try {
			parseInsert(db, insert);
			fail("Should have complained about missing values");
		} catch (TransformException te) {			
		}
	}
	
	@Test
	public void invalidMultiValuesSizes() throws Exception {
		SchemaContext db= buildNonIncrInsertSchema();
		String insert = "insert into nonincrinsert (id, counter) values (1, 1), (2, 2), (3, 3), (4)";
		try {
			parseInsert(db, insert);
			fail("Should have complained about differing numbers of values");
		} catch (TransformException te) {			
		}
	}

	private static final String[] defaultValueSchema = new String[] {
		"create table defval (`id` int auto_increment, `aval` int default 1, `bval` int default 2);"
	};
	
	@Test
	public void defaultValues() throws Exception {
		SchemaContext db = buildDatabase(TestName.MULTI,defaultValueSchema);
		String in = "insert into defval (aval) values(default)";
		InsertIntoValuesStatement is = parseInsert(db, in);
		PETable tab = is.getTableInstance().getAbstractTable().asTable();
		assertColumnsAre(db, is.getColumnSpecification(), tab, new String[] { "aval", "id", "bval" });
		assertValuesAre(db, is.getValues().get(0), new Object[] { "1", new Long(1), "2" });
	}
	
	@Test
	public void testPE762() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe762 (`id` int, `fid` int) range distribute on (`id`) using openrange");
		stmtTest(db,
				"insert into pe762 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6)",
				InsertStatement.class,
				null);
	}
}
