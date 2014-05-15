// OS_STATUS: public
package com.tesora.dve.sql.parser;

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
