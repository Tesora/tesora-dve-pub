// OS_STATUS: public
package com.tesora.dve.sql.expression;

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

import com.tesora.dve.sql.node.expression.ExpressionNode;

public class ExpressionKey extends RewriteKey {

	protected ExpressionNode embedded;
	
	public ExpressionKey(ExpressionNode em) {
		super();
		embedded = em;
	}
	
	public ExpressionNode getExpression() {
		return embedded;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (obj instanceof ExpressionKey) {
			// use schema equals - rewrite keys are about schema equality, not object equality
			ExpressionKey ek = (ExpressionKey) obj;
			return ek.embedded.isSchemaEqual(embedded);
		}
		return false;
	}

	@Override
	public String toString() {
		return "ExpressionKey{" + embedded + "}";
	}

	@Override
	public ExpressionNode toInstance() {
		return embedded;
	}

	@Override
	protected int computeHashCode() {
		return embedded.getSchemaHashCode();
	}
	
}
