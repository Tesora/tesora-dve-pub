package com.tesora.dve.sql.transform.constraints;

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

import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.MatchableKey;
import com.tesora.dve.sql.util.ListSet;

// must all be for the same underlying constraint
public class ConstraintCollection implements PlanningConstraint {
	
	private ListSet<PlanningConstraint> constraints;
	
	public ConstraintCollection() {
		constraints = new ListSet<PlanningConstraint>();
	}
	
	private PlanningConstraint first() {
		return constraints.get(0);
	}
	
	public void add(PlanningConstraint c) {
		if (!constraints.isEmpty()) {
			if (c.sameUnderlyingKey(first())) {
				constraints.add(c);
			} else {
				throw new SchemaException(Pass.PLANNER, "Attempt to create constraint collection of mismatched keys");
			}
		} else
			constraints.add(c);
	}
	
	@Override
	public int compareTo(PlanningConstraint o) {
		return KeyConstraint.compareTo(first(), o);
	}

	@Override
	public PlanningConstraintType getType() {
		return first().getType();
	}

	@Override
	public MatchableKey getKey(SchemaContext sc) {
		return first().getKey(sc);
	}
	
	@Override
	public List<PEColumn> getColumns(SchemaContext sc) {
		return first().getColumns(sc);
	}

	@Override
	public Map<PEColumn, ConstantExpression> getValues() {
		throw new SchemaException(Pass.PLANNER, "Illegal operation: getValues on collection of constraints");
	}

	@Override
	public PEAbstractTable<?> getTable() {
		return first().getTable();
	}

	@Override
	public ExpressionPath getPath() {
		throw new SchemaException(Pass.PLANNER, "Illegal operation: getPath on a collection of constraints");
	}

	@Override
	public boolean sameUnderlyingKey(PlanningConstraint other) {
		return first().sameUnderlyingKey(other);
	}

	@Override
	public String toString() {
		return "ConstraintCollection{" + constraints.get(0) + "}"; 
	}

	@Override
	public TableKey getTableKey() {
		return first().getTableKey();
	}

	@Override
	public long getRowCount() {
		// sum of underlying
		long acc = 0;
		for(PlanningConstraint pc : constraints) {
			long irc = pc.getRowCount();
			if (irc == -1) 
				// unknown
				return -1;
			acc += irc;
		}
		return acc;
	}
	
}
