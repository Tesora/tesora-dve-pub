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
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.util.ListSet;

class ExplodingBufferEntry extends BufferEntry {

	protected ListSet<ExpressionNode> parts;
	protected List<ExpressionPath> paths;
	
	public ExplodingBufferEntry(ExpressionNode targ, ListSet<ExpressionNode> bits) {
		super(targ);
		parts = bits;
		paths = new ArrayList<ExpressionPath>();
		for(ExpressionNode ci : parts) 
			paths.add(ExpressionPath.build(ci, targ));
	}

	@Override
	public List<ExpressionNode> getNext() {
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(ExpressionNode ci : parts)
			out.add((ExpressionNode) ci.copy(null));
		return out;
	}
	
	@Override
	public ExpressionNode buildNew(SchemaContext sc, SchemaMapper in) {
		ExpressionNode copy = (ExpressionNode) getTarget().copy(new CopyContext("ExplodingBuffer"));
		for(int i = 0; i < parts.size(); i++) {
			ExpressionNode was = parts.get(i);
			ColumnInstance nci = null;
			ExpressionPath ep = paths.get(i);
			if (was instanceof ColumnInstance) {
				nci = in.copyForward((ColumnInstance)was);
			} else {
				nci = buildNewCompoundRedist(sc, in, was);
			}
			ep.update(copy,nci);
		}
		return copy;
	}
	
	@Override
	public ExpressionNode buildNew(List<ExpressionNode> intermediateProjection) {
		ExpressionNode copy = (ExpressionNode) getTarget().copy(null);
		for(int i = 0; i < parts.size(); i++) {
			ExpressionNode repl = ExpressionUtils.getTarget(intermediateProjection.get(getAfterOffsetBegin() + i));
			ExpressionPath ep = paths.get(i);
			ep.update(copy,repl);
		}
		return copy;
	}	
	
	@Override
	public boolean isCompound() {
		return true;
	}
}