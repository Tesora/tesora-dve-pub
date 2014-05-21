// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public class NullRejectionSimplifier extends Simplifier {

	@Override
	public boolean applies(SchemaContext sc, DMLStatement stmt) throws PEException {
		if (EngineConstant.FROMCLAUSE.has(stmt)) {
			AnyOuterJoinTraversal any = new AnyOuterJoinTraversal();
			any.traverse(EngineConstant.FROMCLAUSE.getEdge(stmt));
			if (any.hasLeftOuterJoins()) return true;
		}
		return false;
	}

	@Override
	public DMLStatement simplify(SchemaContext sc, DMLStatement in, JoinSimplificationTransformFactory parent) throws PEException {
		String before = (parent.emitting() ? in.getSQL(sc) : null);
		if (process(sc, in)) {
			if (parent.emitting()) {
				parent.emit(" NR in:  " + before);
				parent.emit(" NR out: " + in.getSQL(sc));
			}
			return in;
		}
		return null;
	}

	public static boolean process(SchemaContext sc, DMLStatement stmt) {

		boolean mods = false;
		boolean done = false;
		
		NRBuilder wcbuilder = null;
		do {
			// build the first otab set
			JoinsTraversal ott = new JoinsTraversal();
			ott.traverse(EngineConstant.FROMCLAUSE.getEdge(stmt));
			Map<TableKey,JoinedTable> otabs = ott.getOuterTables();
			List<TableKey> oj = new ArrayList<TableKey>();
			for(Map.Entry<TableKey,JoinedTable> me : otabs.entrySet()) {
				if (me.getValue().getJoinType().isLeftOuterJoin())
					oj.add(me.getKey());
			}
			if (oj.isEmpty()) 
				return mods;
			if (wcbuilder == null) {
				wcbuilder = new NRBuilder(sc,true,(ExpressionNode)EngineConstant.WHERECLAUSE.get(stmt));
				if (wcbuilder.isUnsupported())
					return false;
			}
			List<TableKey> any = wcbuilder.required(oj); 
			if (any.isEmpty()) {
				// next up, join conditions
				List<ExpressionNode> roots = new ArrayList<ExpressionNode>();
				for(Map.Entry<TableKey, JoinedTable> me : otabs.entrySet()) {
					if (me.getValue().getJoinType().isInnerJoin()) {
						if (me.getValue().getJoinOn() != null)
							roots.add(me.getValue().getJoinOn());
					}
				}
				if (!roots.isEmpty()) {
					NRBuilder jb = new NRBuilder(sc,false,roots);
					if (!jb.isUnsupported())
						any = jb.required(oj);
				}
			}
			if (!any.isEmpty()) {
				for(TableKey tk : any) {
					JoinedTable enclosing = otabs.get(tk);
					if (enclosing.getJoinType().isLeftOuterJoin()) 
						enclosing.setJoinType(JoinSpecification.INNER_JOIN);
				}
				mods = true;
			}
			else
				done = true;
		} while(!done);

		return mods;
	}
	
	private static class JoinsTraversal extends Traversal {

		LinkedHashMap<TableKey,JoinedTable> tables = new LinkedHashMap<TableKey,JoinedTable>();
		
		
		public JoinsTraversal() {
			super(Order.PREORDER, ExecStyle.ONCE);
		}
		
		public Map<TableKey,JoinedTable> getOuterTables() {
			return tables;
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof JoinedTable) {
				JoinedTable jt = (JoinedTable) in;
				if (jt.getJoinedToTable() != null)
					tables.put(jt.getJoinedToTable().getTableKey(), jt);
			}
			return in;
		}	
	}
	
	private static class AnyOuterJoinTraversal extends Traversal {
		
		private boolean any = false;
		
		public AnyOuterJoinTraversal() {
			super(Order.PREORDER,ExecStyle.ONCE);
		}
		
		@Override
		public boolean allow(Edge<?,?> e) {
			return !any;
		}
		
		@Override
		public boolean allow(LanguageNode ln) {
			return !any;
		}
		
		public boolean hasLeftOuterJoins() {
			return any;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof JoinedTable) {
				JoinedTable jt = (JoinedTable) in;
				if (jt.getJoinType().isLeftOuterJoin()) {
					any = true;
				}
			}
			return in;
		}
	}

}
