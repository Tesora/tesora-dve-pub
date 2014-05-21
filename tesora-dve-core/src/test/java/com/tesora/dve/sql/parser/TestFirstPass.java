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

import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.ParserOptions;

public class TestFirstPass extends TestParser {

	@Override
	protected ParserInvoker getDefaultInvoker() throws Exception {
		return new FirstPassInvoker(ParserOptions.TEST.setFailEarly());
	}
	
	
	@Test
	public void testSelect() throws Throwable {
		parseOneSqlFile("selects.sql");
	}
	
	@Test
	public void testInsert() throws Throwable {
		parseOneSqlFile("inserts.sql");
	}
	
	@Test
	public void testCreate() throws Throwable {
		parseOneSqlFile("creates.sql");
	}
	
	@Test
	public void testUpdate() throws Throwable {
		parseOneSqlFile("updates.sql");
	}
	
	@Test
	public void testPECreate() throws Throwable {
		parseOneSqlFile("pecreates.sql");
	}
	
	@Test
	public void testDelete() throws Throwable {
		parseOneSqlFile("deletes.sql");
	}	
}
