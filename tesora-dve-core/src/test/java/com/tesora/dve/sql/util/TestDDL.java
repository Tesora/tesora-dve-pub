// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.util.List;

import com.tesora.dve.sql.parser.ParserInvoker.LineTag;
import com.tesora.dve.sql.parser.ParserInvoker.TaggedLineInfo;

public abstract class TestDDL {

	protected boolean created = false;
	
	public abstract List<String> getCreateStatements() throws Exception;
	
	public abstract List<String> getDestroyStatements() throws Exception;
	
	public void create(TestResource tr) throws Throwable {
		create(tr.getConnection());
	}
	
	public void create(ConnectionResource cr) throws Throwable {
		TaggedLineInfo tli = new TaggedLineInfo(-1,null,-1,LineTag.DDL);
		for(String c : getCreateStatements()) 
			cr.execute(tli,c);
	}

	public void destroy(TestResource tr) throws Throwable {
		destroy(tr.getConnection());
	}
	
	public void destroy(ConnectionResource mr) throws Throwable {
		TaggedLineInfo tli = new TaggedLineInfo(-1,null,-1,LineTag.DDL);
		for(String c : getDestroyStatements())
			mr.execute(tli,c);
	}

	public abstract List<String> getSetupDrops();

	public void clearCreated() {
		created = false;
	}
	
	protected synchronized void setCreated() {
		created = true;
	}
	
	protected synchronized boolean isCreated() {
		return created;
	}
	
}
