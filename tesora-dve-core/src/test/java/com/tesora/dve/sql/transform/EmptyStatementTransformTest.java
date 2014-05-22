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

import org.junit.Test;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.util.TestName;

/**
 * Tests to make sure the parser handles syntax that is actually ignored or handled elsewhere
 * 
 * @author peter
 *
 */
public class EmptyStatementTransformTest extends TransformTest {

	// we don't need a schema
	private static final String[] schema = new String[] {};

	public EmptyStatementTransformTest() {
		super("EmptyStatementTransformTest");
	}

    @Test
    public void testPE1152_GrantProcessIgnored() throws Exception {
        SchemaContext db = buildSchema(TestName.MULTI, schema);
        stmtTest(db, "GRANT process ON *.* TO `f5_db_user`@`%` IDENTIFIED BY 'S'", EmptyStatement.class, null);
        stmtTest(db, "GRANT process ON *.* TO `f5_db_user`@`%`", EmptyStatement.class, null);
    }

}
