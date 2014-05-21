// OS_STATUS: public
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
import com.tesora.dve.sql.util.TestName;

public class PreparedStatementTransformTest extends TransformTest {

	public PreparedStatementTransformTest() {
		super("PreparedStatementTransformTest");
	}
	
	@Test
	public void testDistKeyA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, 
				"create table tdka (`id` int, `sid` int, `oth` varchar(32), primary key (`id`)) static distribute on (`id`)");
		prepareTest(db,
				"select * from tdka where id = ?",
				1,
				null);
	}

	@Test
	public void testDistKeyB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, 
				"create table tdkb (`id` int, `sid` int, `oth` varchar(32), primary key (`id`)) static distribute on (`id`)");
		prepareTest(db,
				"select * from tdkb where id in (?,?,?,?,?)",
				5,
				null);
	}

}
