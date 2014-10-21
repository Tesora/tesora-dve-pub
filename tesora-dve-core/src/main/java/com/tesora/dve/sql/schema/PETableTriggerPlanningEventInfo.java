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

import java.util.Collection;
import java.util.LinkedHashMap;

import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.expression.TriggerTableKey;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;

public class PETableTriggerPlanningEventInfo extends PETableTriggerEventInfo {

	// the before/after columns are across both triggers
	private ListSet<ColumnKey> beforeColumns;
	private ListSet<ColumnKey> afterColumns;
	// the key iteration order defines the temp table result set
	private LinkedHashMap<ColumnKey,Integer> connValueOffsets;

	public PETableTriggerPlanningEventInfo() {
		super();
		beforeColumns = null;
		afterColumns = null;
		connValueOffsets = null;
	}

	private void collectColumns(SchemaContext sc, PETrigger trig, ListSet<ColumnKey> befores, ListSet<ColumnKey> afters) {
		Statement body = trig.getBody(sc);
		ListSet<ColumnKey> referencedColumns = ColumnInstanceCollector.getColumnKeys(body);
		for(ColumnKey ck : referencedColumns) {
			TableKey tk = ck.getTableKey();
			if (tk instanceof TriggerTableKey) {
				TriggerTableKey ttk = (TriggerTableKey) tk;
				if (ttk.isBefore())
					befores.add(ck);
				else
					afters.add(ck);
			}
		}
	}
	
	private LinkedHashMap<ColumnKey,Integer> buildOffsets(ListSet<ColumnKey> befores, ListSet<ColumnKey> afters) {
		LinkedHashMap<ColumnKey,Integer> out = new LinkedHashMap<ColumnKey,Integer>();
		int offset = -1;
		for(ColumnKey ck : befores)
			out.put(ck,++offset);
		for(ColumnKey ck : afters)
			out.put(ck, ++offset);
		return out;
	}

	private void ensureRuntime(SchemaContext sc) {
		if (connValueOffsets == null) {
			ListSet<ColumnKey> befores = new ListSet<ColumnKey>();
			ListSet<ColumnKey> afters = new ListSet<ColumnKey>();
			for(PETrigger trig : get()) {
				collectColumns(sc,trig,befores,afters);
			}
			LinkedHashMap<ColumnKey,Integer> offsets = buildOffsets(befores,afters);
			if (connValueOffsets == null) {
				synchronized(this) {
					if (connValueOffsets == null) {
						connValueOffsets = offsets;
						beforeColumns = befores;
						afterColumns = afters;
					}
				}
			}
		}
	}
	
	@Override
	public Collection<ColumnKey> getTriggerBodyColumns(SchemaContext sc) {
		ensureRuntime(sc);
		return connValueOffsets.keySet();
	}

	
	
}
