package com.tesora.dve.sql.schema;

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
import java.util.Collection;

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ColumnKey;

public class PETableTriggerEventInfo {

	private PETrigger before;
	private PETrigger after;
	
	public PETableTriggerEventInfo() {
		before = null;
		after = null;
	}
	
	public void set(PETrigger trig) {
		if (trig.isBefore())
			before = trig;
		else
			after = trig;
	}
	
	public void remove(PETrigger trig) {
		if (trig.isBefore())
			before = null;
		else
			after = null;
	}
	
	public PETrigger getBefore() {
		return before;
	}

	public PETrigger getAfter() {
		return after;
	}

	public Collection<PETrigger> get() {
		ArrayList<PETrigger> out = new ArrayList<PETrigger>();
		if (before != null) out.add(before);
		if (after != null) out.add(after);
		return out;
	}
	
	public Collection<ColumnKey> getTriggerBodyColumns(SchemaContext sc) {
		throw new SchemaException(Pass.PLANNER, "Invalid trigger planning");
	}
	
}
