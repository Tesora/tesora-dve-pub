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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.TestName;

public class SelectNormalizationTest extends TransientSchemaTest {

	public SelectNormalizationTest() {
		super("NormalizeSelect");
	}
	
	private static final String[] sqlschema = new String[] { 
		"create table foo (`id` integer unsigned not null, `payload` varchar(50), flags tinyint) static distribute on (`id`);",
		"create table bar (`id` integer unsigned not null, `description` varchar(50)) static distribute on (`id`);"
	};
	
	public SchemaContext buildDatabase() throws Exception {
		return buildDatabase(TestName.MULTI,sqlschema);
	}
	
	private SelectStatement parseSelect(SchemaContext db, String in) throws Exception {
		List<Statement> stmts = parse(db, in);
		assertTrue(stmts.size() == 1);
		SelectStatement ss = (SelectStatement) stmts.get(0);
		ss.normalize(db);
		return ss;
	}
	
	private void assertExpressionAliases(List<ExpressionNode> expr) {
		assertTrue(Functional.all(expr, ExpressionAlias.instanceTest));
	}
	
	@Test
	public void wildcardExpansion() throws Exception {
		SchemaContext db = buildDatabase();
		String sql = "select * from foo;";
		SelectStatement ss = parseSelect(db, sql);
		assertEquals("projection size",3,ss.getProjection().size());
		assertExpressionAliases(ss.getProjection());
	}
	
	@Test
	public void wildcardTableExpansionA() throws Exception {
		SchemaContext db = buildDatabase();
		String sql = "select f.* from foo f;";
		SelectStatement ss = parseSelect(db, sql);
		assertEquals("projection size",3,ss.getProjection().size());
		assertExpressionAliases(ss.getProjection());
	}
	
	@Test
	public void wildcardTableExpansionB() throws Exception {
		SchemaContext db = buildDatabase();
		String sql = "select f.*, b.* from foo f, bar b";
		SelectStatement ss = parseSelect(db, sql);
		assertEquals("projection size",5,ss.getProjection().size());
		assertExpressionAliases(ss.getProjection());
	}
	
	@Test
	public void projectionAliases() throws Exception {
		SchemaContext db = buildDatabase();
		String sql = "select id, payload, flags f from foo;";
		SelectStatement ss = parseSelect(db, sql);
		assertEquals("projection size",3,ss.getProjection().size());
		assertExpressionAliases(ss.getProjection());		
	}
	
	@Test
	public void ambiguousPrimaryTable() throws Exception {
		SchemaContext db = buildDatabase();
		String sql = "select * from foo f, bar b where f.id = b.id;";
		SelectStatement ss = parseSelect(db,sql);
		assertEquals("projection size",5,ss.getProjection().size());
		assertExpressionAliases(ss.getProjection());
	}	
	
	@Test
	public void testExplicitJoin() throws Exception {
		SchemaContext db = buildDatabase();
		SelectStatement ss = parseSelect(db, "select * from foo f inner join bar b on f.id = b.id where b.description = 'wha'");
		assertEquals("projection size",5,ss.getProjection().size());
		assertExpressionAliases(ss.getProjection());
	}
}
