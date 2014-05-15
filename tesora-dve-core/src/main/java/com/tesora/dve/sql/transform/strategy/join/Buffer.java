// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;

abstract class Buffer {
	
	protected List<BufferEntry> entries;
	protected Buffer prev;
	protected final BufferKind kind;
	
	public Buffer(BufferKind bk, Buffer bef) {
		entries = new ArrayList<BufferEntry>();
		prev = bef;
		kind = bk;
	}
	
	public void add(BufferEntry be) {
		be.setBeforeOffset(entries.size());
		entries.add(be);
	}
	
	public int size() {
		return entries.size();
	}
	
	public List<BufferEntry> getEntries() {
		return entries;
	}
	
	public Buffer getPreviousBuffer() {
		return prev;
	}

	public Buffer getBuffer(BufferKind bk) {
		if (kind == bk) 
			return this;
		else if (prev == null)
			return null;
		else
			return prev.getBuffer(bk);
	}
	
	public abstract void adapt(SchemaContext sc, SelectStatement stmt) throws PEException;		
}