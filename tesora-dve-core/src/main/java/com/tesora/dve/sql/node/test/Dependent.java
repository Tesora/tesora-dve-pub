// OS_STATUS: public
package com.tesora.dve.sql.node.test;

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

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class Dependent extends DerivedAttribute<ListSet<TableKey>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return true;
	}

	@Override
	public ListSet<TableKey> computeValue(SchemaContext sc, LanguageNode ln) {
		ListSet<TableKey> out = new ListSet<TableKey>();
		if (EngineConstant.COLUMN.has(ln)) {
			ColumnInstance ci = (ColumnInstance) ln;
			out.add(ci.getTableInstance().getTableKey());
		} else if (EngineConstant.CONSTANT.has(ln)) {
			// constants don't depend on anything
		} else if (EngineConstant.TABLE.has(ln)) {
			TableInstance ti = (TableInstance) ln;
			out.add(ti.getTableKey());
		} else {
			for(Edge<?,? extends LanguageNode> e : ln.getEdges()) {
				if (e.isMulti()) {
					MultiEdge<?, ? extends LanguageNode> multi = (MultiEdge<?, ? extends LanguageNode>) e;
					for(LanguageNode sn : multi.getMulti()) {
						out.addAll(EngineConstant.DEPENDENT.getValue(sn,sc));
					}
				} else {
					SingleEdge<?, ? extends LanguageNode> single = (SingleEdge<?, ? extends LanguageNode>) e;
					out.addAll(EngineConstant.DEPENDENT.getValue(single.get(),sc));
				}
			}
		}
		return out;
	}

}
