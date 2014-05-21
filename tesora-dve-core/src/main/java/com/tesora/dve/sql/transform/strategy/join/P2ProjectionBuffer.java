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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;

class P2ProjectionBuffer extends FinalBuffer {
	
	private Map<RewriteKey,BufferEntry> bufsForCols;
	
	public P2ProjectionBuffer(Buffer prev, PartitionLookup lookup) {
		super(BufferKind.P2, prev, lookup);
		bufsForCols = new HashMap<RewriteKey,BufferEntry>();
	}

	@Override
	public void adapt(SchemaContext sc, SelectStatement stmt) {
		for(BufferEntry be : getPreviousBuffer().getEntries()) {
			be.setAfterOffsetBegin(size());
			List<ExpressionNode> nexts = be.getNext();
			for(ExpressionNode en : nexts) {
				RewriteKey enrk = en.getRewriteKey();
				BufferEntry nbe = new BufferEntry(en);
				if (!bufsForCols.containsKey(enrk)) {
					bufsForCols.put(enrk, nbe);
				}
				be.addDependency(nbe);
				add(nbe);
			}
			be.setAfterOffsetEnd(size());
		}
	}
	
	public BufferEntry add(ColumnInstance ci) {
		BufferEntry be = bufsForCols.get(ci.getColumnKey());
		if (be == null) {
			be = new BufferEntry((ExpressionNode)ci.copy(null));
			bufsForCols.put(ci.getColumnKey(), be);
			add(be);
		}
		return be;
	}

	public BufferEntry addForJoin(ExpressionNode en, boolean redistReqd) {
		RewriteKey rk = en.getRewriteKey();
		BufferEntry be = bufsForCols.get(rk);
		if (be == null) {
			ExpressionNode copy = (ExpressionNode)en.copy(null); 
			if (redistReqd)
				be = new RedistBufferEntry(copy);
			else
				be = new BufferEntry(copy);
			bufsForCols.put(rk, be);
			add(be);
		}
		return be;
	}
	
}