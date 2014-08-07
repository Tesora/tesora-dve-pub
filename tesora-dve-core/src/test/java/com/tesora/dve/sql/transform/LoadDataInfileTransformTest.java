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

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.session.LoadDataInfileStatement;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.util.TestName;

public class LoadDataInfileTransformTest extends TransformTest {

	public LoadDataInfileTransformTest() {
		super("EMPTY LOAD DATA INFILE");
	}

	private static final String[] leftySchema = new String[] { "create table `A` (`id` int unsigned not null, `col1` varchar(50) not null, `id2` int, `col2` varchar(10) ) random distribute", };

	@Ignore
	@Test
	public void basicTest() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, leftySchema);
//		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(
				db,
				"LOAD DATA LOCAL INFILE 'd:/vmshare/loaddata.dat' IGNORE INTO TABLE `a` FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' (`id`, `field`)",
				LoadDataInfileStatement.class,
				bes(new ExpectedStep(EmptyExecutionStep.class, null, projectName)));
	}
}
