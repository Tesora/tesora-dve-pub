package com.tesora.dve.sql.transform.strategy.join;

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