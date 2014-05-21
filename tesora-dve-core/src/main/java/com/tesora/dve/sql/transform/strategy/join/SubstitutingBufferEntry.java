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

import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;

public class SubstitutingBufferEntry extends BufferEntry {

	List<ExpressionNode> repl;
	
	public SubstitutingBufferEntry(ExpressionNode targ, List<ColumnInstance> replacementExprs) {
		super(targ);
		repl = new ArrayList<ExpressionNode>();
		for(ColumnInstance ci : replacementExprs)
			repl.add(ci);
	}

	@Override
	public List<ExpressionNode> getNext() {
		return repl;
	}

	@Override
	public ExpressionNode buildNew(List<ExpressionNode> intermediateProjection) {
		return getTarget();
	}
	
}
