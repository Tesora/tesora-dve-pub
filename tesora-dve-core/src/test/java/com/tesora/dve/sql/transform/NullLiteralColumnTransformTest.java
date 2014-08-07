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

import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class NullLiteralColumnTransformTest extends TransformTest {

	public NullLiteralColumnTransformTest() {
		super("NullLiteralColumnTransformTest");
	}

	private static final String[] leftySchema = new String[] { "create table `A` (`id` int unsigned not null, `col1` varchar(50) not null, `id2` int, `col2` varchar(10) ) random distribute", };

	@Test
	public void redistPlanTest() throws Throwable {
		// cause a redist by selecting everything from the random tables and
		// order by
		SchemaContext db = buildSchema(TestName.MULTI, leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"select id, col1, id2, col2 from A order by id",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT `A`.`id` AS A1i0_6,`A`.`col1` AS A1c1_7,`A`.`id2` AS A1i2_8,`A`.`col2` AS A1c3_9",
						  "FROM `A`"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT temp1.A1i0_6 AS t2A0,temp1.A1c1_7 AS t2A1,temp1.A1i2_8 AS t2A2,temp1.A1c3_9 AS t2A3",
						  "FROM temp1",
						  "ORDER BY t2A0 ASC"
						)
					)
				);
	}
}
