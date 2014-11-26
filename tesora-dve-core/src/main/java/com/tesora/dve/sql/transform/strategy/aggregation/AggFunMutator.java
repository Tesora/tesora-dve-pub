package com.tesora.dve.sql.transform.strategy.aggregation;

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

import java.util.Collections;
import java.util.Map;

import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;

abstract class AggFunMutator extends ColumnMutator {
	
	protected final FunctionName fn;
	protected SetQuantifier quantifier;
	
	protected AggFunMutator(final FunctionName fn) {
		this.fn = fn;
	}

	public boolean isDistinct() {
		return SetQuantifier.DISTINCT == quantifier;
	}
	
	public boolean requiresNoGroupingFirstPass() {
		return false;
	}
	
	public Map<AggFunMutator, ExpressionNode> getChildren() {
		return Collections.EMPTY_MAP;
	}

	public final boolean hasChildren() {
		final Map<AggFunMutator, ExpressionNode> children = this.getChildren();
		return ((children != null) && !children.isEmpty());
	}

	public FunctionName getFunctionName() {
		return this.fn;
	}

}