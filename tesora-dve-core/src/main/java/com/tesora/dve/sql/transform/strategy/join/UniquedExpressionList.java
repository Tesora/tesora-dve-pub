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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;

public class UniquedExpressionList {
	
	private List<ExpressionNode> exprs;
	private Set<RewriteKey> keys;
	
	public UniquedExpressionList() {
		exprs = new ArrayList<ExpressionNode>();
		keys = new HashSet<RewriteKey>();
	}

	public List<ExpressionNode> getExprs() {
		return exprs;
	}
	
	public UniquedExpressionList addAll(Collection<ExpressionNode> c) {
		if (c == null) return this;
		for(ExpressionNode en : c)
			add(en);
		return this;
	}
	
	public UniquedExpressionList add(ExpressionNode en) {
		if (en == null) return this;
		RewriteKey rk = en.getRewriteKey();
		if (keys.add(rk))
			exprs.add(en);
		return this;
	}
}