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

import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.TransformTest;
import com.tesora.dve.sql.util.TestName;

public class AdhocParserTest extends TransformTest {

	public AdhocParserTest() {
		super("AdhocParserTest");
	}
	
	@Test
	public void testPE26() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table t1 (a int)");
		parse(db, "select * from t1 where (1 and a)");
		parse(db, "select * from t1 where NOT (1 and a)");		
	}
	
	@Test
	public void testPE1486() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe1486 (`phase` int)");
		parse(db, "select phase from pe1486");
	}
}
