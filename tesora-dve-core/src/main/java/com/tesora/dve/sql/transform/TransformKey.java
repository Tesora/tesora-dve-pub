package com.tesora.dve.sql.transform;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.distribution.ColumnDatum;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.util.ListOfPairs;

public abstract class TransformKey {

	// the containing expression
	protected ExpressionNode containing;
	
	protected TransformKey(ExpressionNode containing) {
		this.containing = containing;
	}
	
	public ExpressionNode getContaining() { return containing; }
	
	public abstract List<TransformKeySimple> getKeySource();
	public abstract boolean valid(SchemaContext sc);
	public abstract TableInstance getTable();
	public abstract boolean hasParameterizedValues();

	public static class TransformKeySimple extends TransformKey {
		
		protected List<TransformKeyValueColumn> values;
		protected TableInstance onTable;
		
		public TransformKeySimple(ExpressionNode containing, ColumnInstance col, ConstantExpression lit) { 
			super(containing);
			values = new ArrayList<TransformKeyValueColumn>();
			values.add(new TransformKeyValueColumn(col, lit));
			onTable = col.getTableInstance();
		}

		public TransformKeySimple(ExpressionNode containing, List<TransformKeySimple> composedOf) {
			super(containing);
			values = new ArrayList<TransformKeyValueColumn>();
			for(TransformKeySimple tks : composedOf)
				values.addAll(tks.getValues());
			onTable = values.get(0).getTable();
		}
		
		public List<TransformKeyValueColumn> getValues() {
			return values;
		}
		
		@Override
		public boolean hasParameterizedValues() {
			for(TransformKeyValueColumn tkvc : values)
				if (tkvc.isParameterized())
					return true;
			return false;
		}
		
		@Override
		public TableInstance getTable() {
			return onTable;
		}
		
		@Override
		public List<TransformKeySimple> getKeySource() {
			return Collections.singletonList(this);
		}

		@Override
		public boolean valid(SchemaContext sc) {
			TableInstance ti = null;
			HashSet<Column<?>> columns = new HashSet<Column<?>>();
			for(TransformKeyValueColumn c : values) {
				columns.add(c.getColumn().getColumn());
				if (ti == null)
					ti = c.getTable();
				else if (ti != c.getTable())
					return false;
			}
			if (!columns.equals(ti.getAbstractTable().getDistributionVector(sc).getColumns(sc)))
				return false;
			return true;
		}

		public DistributionKey buildKeyValue(SchemaContext sc) {
			PEAbstractTable<?> tab = onTable.getAbstractTable();
			ListOfPairs<PEColumn,ConstantExpression> vect = new ListOfPairs<PEColumn,ConstantExpression>();
			for(TransformKeyValueColumn vc : values)
				vect.add(vc.getColumn().getPEColumn(),vc.getLiteral());
			DistributionKey kv = tab.getDistributionVector(sc).buildDistKey(sc, onTable.getTableKey(), vect); 
			return kv;
		}
		
	}
	
	// a series key involves many actual key values
	public static class TransformKeySeries extends TransformKey {
		
		protected List<TransformKeySimple> parts;
		
		public TransformKeySeries(ExpressionNode containing, List<TransformKeySimple> parts) {
			super(containing);
			this.parts = parts;
		}

		@Override
		public List<TransformKeySimple> getKeySource() {
			return parts;
		}

		@Override
		public boolean hasParameterizedValues() {
			for(TransformKeySimple tks : parts)
				if (tks.hasParameterizedValues())
					return true;
			return false;
		}
		
		@Override
		public boolean valid(SchemaContext sc) {
			boolean valid = true;
			TableInstance ti = null;
			for(TransformKeySimple tks : parts) {
				if (!tks.valid(sc)) {
					valid = false;
					break;
				} else if (ti == null) 
					ti = tks.getTable();
				else if (ti != tks.getTable()) {
					valid = false;
					break;
				}
			}
			return valid;
		}

		@Override
		public TableInstance getTable() {
			return parts.get(0).getTable();
		}
		
	}
	
	public static class TransformKeyValueColumn {
				
		protected ColumnInstance column;
		protected ConstantExpression literal;
				
		public TransformKeyValueColumn(ColumnInstance ci, ConstantExpression lit) {
			column = ci;
			literal = lit;
		}
				
		public ColumnInstance getColumn() {
			return column;
		}

		public TableInstance getTable() {
			return column.getTableInstance();
		}

		public ConstantExpression getLiteral() {
			return literal;
		}

		public boolean isParameterized() {
			return literal instanceof IParameter;
		}
		
	}
}
