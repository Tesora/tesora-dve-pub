package com.tesora.dve.upgrade;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.util.Functional;

public class SchemaState {

	private Map<String,TableState> state;
	
	public SchemaState() {
		this.state = new HashMap<String,TableState>();
	}
	
	public void build(DBHelper helper, List<String> tableNames) throws Throwable {
		for(String tn : tableNames) 
			state.put(tn, new TableState(helper, PEConstants.CATALOG, tn));
	}
	
	public String differs(SchemaState other) {
		List<String> messages = new ArrayList<String>();
		for(Map.Entry<String, TableState> me : state.entrySet()) {
			TableState mine = me.getValue();
			TableState yours = other.state.get(me.getKey());
			String diffs = mine.differs(yours);
			if (diffs != null)
				messages.add("On table " + me.getKey() + ": " + diffs);
		}
		if (messages.isEmpty()) return null;
		return Functional.join(messages, System.getProperty("line.separator"));
	}
	
}
