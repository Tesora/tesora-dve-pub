// OS_STATUS: public
package com.tesora.dve.sql.util;

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
