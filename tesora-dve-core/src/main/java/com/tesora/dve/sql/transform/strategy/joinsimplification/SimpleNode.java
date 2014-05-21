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

import java.util.Collections;
import java.util.Set;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;

// either a constant or a column reference
public class SimpleNode extends NRNode {

	public SimpleNode(ExpressionNode en) {
		super(en);
	}

	@Override
	public boolean required(TableKey tab) {
		if (wrapping instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) wrapping;
			return (ci.getTableInstance().getTableKey().equals(tab));
		}
		// otherwise a constant
		return false;
	}

	@Override
	protected Set<TableKey> computeUses() {
		if (wrapping instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) wrapping;
			return Collections.singleton(ci.getTableInstance().getTableKey());
		}
		return Collections.emptySet();
	}

}
