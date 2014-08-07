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

import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.util.TestName;

public class DeleteTransformTest extends TransformTest {

	public DeleteTransformTest() {
		super("DeleteTransformTest");
	}
	
	@Test
	public void testPE610() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table pe610a (`id` int, `fid` int, primary key (`id`)) random distribute",
				"create table pe610b (`id` int, `fid` int, primary key (`id`)) broadcast distribute");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"delete p from pe610a p inner join pe610b q on p.fid = q.fid where q.id = 15",
				DeleteStatement.class,
				bes(
						new DeleteExpectedStep(group,
						"DELETE p FROM `pe610a` AS p INNER JOIN `pe610b` AS q ON p.`fid` = q.`fid` WHERE q.`id` = 15")
					));
		stmtTest(db,
				"delete pe610a from pe610a inner join pe610b on pe610a.fid = pe610b.fid where pe610b.id = 15",
				DeleteStatement.class,
				bes(
						new DeleteExpectedStep(group,
						"DELETE `pe610a` FROM `pe610a` INNER JOIN `pe610b` ON `pe610a`.`fid` = `pe610b`.`fid` WHERE `pe610b`.`id` = 15")
					));
	}
	
}
