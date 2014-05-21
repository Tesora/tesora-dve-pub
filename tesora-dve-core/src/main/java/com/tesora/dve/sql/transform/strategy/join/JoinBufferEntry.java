// OS_STATUS: public
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

import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.util.ListSet;

public class JoinBufferEntry extends BufferEntry {
	
	protected FromTableReference fromTab;
	protected JoinedTable jt;
	protected ListSet<TableKey> tabs;
	protected ListSet<ExpressionNode> exprs;
	
	public JoinBufferEntry(ExpressionNode joinEx, FromTableReference fromTab, JoinedTable join, ListSet<TableKey> tabKeys, ListSet<ExpressionNode> equiJoinExprs) {
		super(joinEx);
		this.fromTab = fromTab;
		jt = join;
		tabs = tabKeys;
		exprs = equiJoinExprs;
	}
	
	public FromTableReference getBase() {
		return fromTab;
	}
	
	public JoinedTable getJoinedTable() {
		return jt;
	}
	
	public ListSet<TableKey> getTableKeys() {
		return tabs;
	}
	
	public TableKey getJoinedToKey() {
		return jt.getJoinedToTable().getTableKey();
	}
	
	public TableKey getJoinedFromKey() {
		TableKey to = getJoinedToKey();
		if (tabs.get(0).equals(to))
			return tabs.get(1);
		return tabs.get(0);
	}
	
	// update the target expression
	public ExpressionNode buildNew(SchemaContext sc, SchemaMapper sm) {
		if (getTarget() == null) return null;
		List<ExpressionNode> decomp = ExpressionUtils.decomposeAndClause(getTarget());
		List<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(ExpressionNode en : decomp)
			out.add((ExpressionNode)en.copy(null));
		IndexCollector ic = new IndexCollector();
		for(ExpressionNode en : exprs) {
			ExpressionPath thePath = null;
			int offset = -1;
			for(int i = 0; i < decomp.size(); i++) {
				ExpressionPath ep = ExpressionPath.build(en, decomp.get(i));
				if (ep == null) continue;
				thePath = ep;
				offset = i;
				break;
			}
			if (en instanceof ColumnInstance) {
				ColumnInstance nci = sm.copyForward((ColumnInstance)en);
				ic.addColumnInstance(nci);
				thePath.update(out.get(offset), nci);
			} else {
				ColumnInstance nci = buildNewCompoundRedist(sc,sm,en);
				ic.addColumnInstance(nci);
				thePath.update(out.get(offset), nci);
			}
		}
		ic.setIndexes(sc);
		return ExpressionUtils.safeBuildAnd(out);
	}
	
	@Override
	public String toString() {
		return "JoinBufferEntry{" + jt.getJoinType().getSQL() + (node == null ? "" : " on " + node.toString()) + "}";
	}	
	
}