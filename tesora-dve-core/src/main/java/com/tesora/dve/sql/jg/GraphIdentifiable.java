// OS_STATUS: public
package com.tesora.dve.sql.jg;

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

import com.tesora.dve.sql.schema.SchemaContext;

public abstract class GraphIdentifiable {

	protected int id;
	
	protected GraphIdentifiable(int gid) {
		id = gid;
	}

	public int getGraphID() {
		return id;
	}
	
	public abstract String getGraphRole();
	
	protected abstract void describeInternal(SchemaContext sc, String ident, StringBuilder buf);
	
	protected void describeSelf(SchemaContext sc, String indent, StringBuilder buf) {
		buf.append(indent).append(getGraphRole()).append("@").append(getGraphID()).append(" ");		
	}
	
	public void describe(SchemaContext sc, String indent, StringBuilder buf) {
		describeSelf(sc,indent,buf);
		describeInternal(sc,indent,buf);		
	}
	
	public String describe(SchemaContext sc, String indent) {
		StringBuilder buf = new StringBuilder();
		describe(sc,indent,buf);
		return buf.toString();
	}
	
	@Override
	public String toString() {
		SchemaContext sc = SchemaContext.threadContext.get();
		if (sc == null) return "no context available";
		StringBuilder buf = new StringBuilder();
		describe(sc,"",buf);
		return buf.toString();
	}
}
