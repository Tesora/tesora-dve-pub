// OS_STATUS: public
package com.tesora.dve.sql.jg;

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
