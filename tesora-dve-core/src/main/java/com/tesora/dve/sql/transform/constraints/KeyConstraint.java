// OS_STATUS: public
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.MatchableKey;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class KeyConstraint implements PlanningConstraint {

	protected PEKey key;
	protected TableKey table;
	protected Map<PEColumn, ConstantExpression> values;
	protected ExpressionPath path;
	protected long rowCount;
	
	public KeyConstraint(SchemaContext sc, PEKey theKey, TableKey theTable, ListOfPairs<PEColumn, ConstantExpression> filters) {
		key = theKey;
		table = theTable;
		values = new HashMap<PEColumn, ConstantExpression>();
		for(Pair<PEColumn,ConstantExpression> p : filters)
			values.put(p.getFirst(),p.getSecond());
		if (values.keySet().size() == key.getKeyColumns().size())
			rowCount = theKey.getCardRatio(sc);
		else {
			// partial match, use column with smallest card ratio
			long sofar = -1;
			for(PEColumn pec : values.keySet()) {
				long ratio = theKey.getCardinality(sc, pec);
				if (ratio < 0) continue;
				if (sofar == -1) sofar = ratio;
				else if (ratio < sofar) sofar = ratio;
			}
			rowCount = sofar;
		}
	}
	
	@Override
	public PlanningConstraintType getType() {
		return key.getPlanningType();
	}

	@Override
	public MatchableKey getKey(SchemaContext sc){
		return key;
	}
	
	@Override
	public List<PEColumn> getColumns(SchemaContext sc){
		return key.getColumns(sc);
	}
	
	@Override
	public PEAbstractTable<?> getTable() {
		return table.getAbstractTable();
	}

	@Override
	public TableKey getTableKey() {
		return table;
	}
	
	@Override
	public ExpressionPath getPath() {
		return path;
	}
	
	@Override
	public Map<PEColumn, ConstantExpression> getValues() {
		return values;
	}

	@Override
	public int compareTo(PlanningConstraint o) {
		return compareTo(this, o);
	}

	public static int compareTo(PlanningConstraint l, PlanningConstraint r) {
		long lrc = l.getRowCount();
		long rrc = r.getRowCount();
		if (lrc > -1 && rrc > -1) {
			if (lrc < rrc)
				return -1;
			else if (lrc > rrc)
				return 1;
			return 0;
		}
		int leftIndex = l.getType().getScore();
		int rightIndex = r.getType().getScore();
		if (leftIndex == rightIndex)
			return 0;
		else if (leftIndex < rightIndex)
			return -1;
		else
			return 1;
	}

	@Override
	public boolean sameUnderlyingKey(PlanningConstraint other) {
		if (other instanceof KeyConstraint) {
			KeyConstraint okc = (KeyConstraint) other;
			return key.equals(okc.key);
		} else if (other instanceof ConstraintCollection) {
			ConstraintCollection cc = (ConstraintCollection)other;
			return cc.sameUnderlyingKey(this);
		}
		return false;
	}

	@Override
	public String toString() {
		return "KeyConstraint(" + key.toString() + ")[" + rowCount + "]";
	}

	@Override
	public long getRowCount() {
		return rowCount;
	}
}
