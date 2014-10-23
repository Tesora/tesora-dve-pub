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

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.TestName;

public class LimitTransformTest extends TransformTest {

	public LimitTransformTest() {
		super("LimitTransformTest");
	}
	
	
	private static final String[] leftySchema = new String[] {
		"create table `titles` (`id` int unsigned not null, `name` varchar(50) not null) static distribute on (`id`)",
		"create table `states` (`id` int unsigned not null, `name` varchar(50) not null, `tag` varchar(50)) static distribute on (`id`)",
		"create table `laws` (`id` int unsigned not null, `state_id` int unsigned not null, `title_id` int unsigned not null, " 
			+ "`status` varchar(16) not null default 'unpublished', `version` int unsigned not null, `law` varchar(100)) "
			+ "static distribute on (`id`)",
		"create table `counties` (`id` int unsigned not null, `name` varchar(50) not null, `state_id` int unsigned not null) " +
			"static distribute on (`id`)",
		"create table `courts` (`id` int unsigned not null, `county_id` int unsigned not null, `address` varchar(50) not null) " +
			"static distribute on (`id`)"
	};
	
	@Test
	public void simpleLimitTestA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select 1 from laws limit 0,1",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(
						"SELECT 1 AS litex_3 FROM `laws` LIMIT 1",
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp1.litex_3 AS t2l0 FROM temp1 LIMIT 0, 1",
							null)
					));
	}
	
	@Ignore
	@Test
	public void testLimitOrderOptimizationA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,leftySchema);
		stmtTest(db,
				"select id from titles order by name limit 10",
				SelectStatement.class,
				null);
	}
	
	@Test
	public void testPStmtA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, leftySchema);
		prepareTest(db,
				"select ? from laws limit 1",
				1,
				bes(
						new ProjectingExpectedStep(
						"SELECT fp0 AS param_3 FROM `laws` LIMIT 1",
							null).withInMemoryLimit()
					));
	}
	
	@Test
	public void testPStmtB() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, leftySchema);
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		prepareTest(db,
				"select ? from laws limit ?",
				2,
				bes(
						new ProjectingExpectedStep(
						"SELECT fp0 AS param_3 FROM `laws`",
							group,"temp2",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
						new ProjectingExpectedStep(
						"SELECT temp2.param_3 AS t2p0 FROM temp2 LIMIT fp1",
							null)
					));
	}

}
