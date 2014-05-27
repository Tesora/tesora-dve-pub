package com.tesora.dve.sql.schema;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.distribution.IColumnDatum;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.distribution.PELockedException;
import com.tesora.dve.distribution.RangeLimit;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.transform.MatchableKey;
import com.tesora.dve.sql.transform.constraints.KeyConstraint;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.transform.constraints.PlanningConstraintType;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class DistributionKey implements PlanningConstraint {

	private final TableKey onTable;
	private final Map<PEColumn, ConstantExpression> values;
	private final List<PEColumn> columnOrder;
	// for random inserts we choose the persistent site ahead of time
	// so we can parallelize multituple inserts
	private PEStorageSite siteOverride;
	
	public DistributionKey(TableKey tab, List<PEColumn> order, ListOfPairs<PEColumn,ConstantExpression> filters) {
		onTable = tab;
		values = new HashMap<PEColumn, ConstantExpression>();
		if (filters != null) {
			for(Pair<PEColumn,ConstantExpression> p : filters)
				values.put(p.getFirst(),p.getSecond());
		}
		columnOrder = order;
		siteOverride = null;
	}
	
	public Model getModel(SchemaContext sc) {
		return onTable.getAbstractTable().getDistributionVector(sc).getModel();
	}

	@Override
	public List<PEColumn> getColumns(SchemaContext sc) {
		return onTable.getAbstractTable().getDistributionVector(sc).getColumns(sc);
	}
	
	public void put(SchemaContext sc, ColumnKey ck, ConstantExpression litex) {
		values.put(ck.getPEColumn(), litex);
	}
	
	public void put(SchemaContext sc, ColumnInstance c, ConstantExpression litex) {
		values.put(c.getPEColumn(), litex);
	}

	public void setGroupOverride(PEStorageSite pegged) {
		siteOverride = pegged;
	}
	
	@Override
	public PlanningConstraintType getType() {
		return PlanningConstraintType.DISTVECT;
	}

	@Override
	public MatchableKey getKey(SchemaContext sc) {
		return onTable.getAbstractTable().getDistributionVector(sc);
	}
	
	@Override
	public Map<PEColumn, ConstantExpression> getValues() {
		return values;
	}


	@Override
	public PEAbstractTable<?> getTable() {
		return onTable.getAbstractTable();
	}

	@Override
	public TableKey getTableKey() {
		return onTable;
	}
	
	@Override
	public ExpressionPath getPath() {
		return null;
	}

	@Override
	public int compareTo(PlanningConstraint o) {
		return KeyConstraint.compareTo(this, o);
	}

	@Override
	public boolean sameUnderlyingKey(PlanningConstraint other) {
		if (other instanceof DistributionKey) {
			DistributionKey odk = (DistributionKey) other;
			return (odk.getTable().equals(getTable()));
		}
		return false;
	}
	
	public String describe(final SchemaContext sc) {
		return "{" + Functional.join(columnOrder, ", ", new UnaryFunction<String, PEColumn>() {
			@Override
			public String evaluate(PEColumn object) {
				ConstantExpression ce = values.get(object);
				return object.getName().get() + "='" + (ce == null ? "unset" : ce.getValue(sc)) + "'";
			}
		}) + "}";
	}

	@Override
	public long getRowCount() {
		return -1;
	}


	public IKeyValue getDetachedKey(SchemaContext sc) {
		// actualize the dist key so that it can be used without a context
		LinkedHashMap<PEColumn, TColumnDatum> vals = new LinkedHashMap<PEColumn, TColumnDatum>();
		for(Map.Entry<PEColumn, ConstantExpression> me : values.entrySet()) {
			vals.put(me.getKey(), new TColumnDatum(me.getKey(),
					me.getValue().convert(sc, me.getKey().getType())));
		}
		return new DetachedKeyValue(sc,onTable.getAbstractTable(),vals,columnOrder,siteOverride);
	}
	
	public void setFrozen() {
		onTable.setFrozen();
		for(ConstantExpression ce : values.values()) {
			ce.setParent(null);
		}
	}
	
	private static class DetachedKeyValue implements IKeyValue {

		private final PEAbstractTable<?> table;
		private final LinkedHashMap<PEColumn, TColumnDatum> values; //NOPMD
		private final SchemaContext context;
		private LinkedHashMap<String, TColumnDatum> externalValues = null; //NOPMD
		private final List<PEColumn> columnOrder;
		private final PEStorageSite pegged;
		
		public DetachedKeyValue(SchemaContext sc, PEAbstractTable<?> ontab, LinkedHashMap<PEColumn,TColumnDatum> vals, List<PEColumn> cols, PEStorageSite peggedGroup) { //NOPMD
			table = ontab;
			values = vals;
			context = sc;
			columnOrder = cols;
			pegged = peggedGroup;
		}
		
		@Override
		public int getUserTableId() {
			return table.getPersistentID();
		}

		@Override
		public String getQualifiedTableName() {
			return table.getQualifiedPersistentName(context);
		}

		@Override
		public int compare(IKeyValue other) throws PEException {
			return KeyValue.compare(this,other);
		}

		@Override
		public int compare(RangeLimit rangeLimit) throws PEException {
			return KeyValue.compare(this, rangeLimit);
		}

		@Override
		public PersistentGroup getPersistentGroup() {
			if (pegged != null) try {
				return new PersistentGroup(pegged.getPersistent(context));
			} catch (PELockedException pele) {
			}
			return table.getPersistent(context).getPersistentGroup();
		}

		@Override
		public int getPersistentGroupId() {
			if (pegged != null)
				// special value - means we have to look up the persistent group anyways
				return -1;
			return table.getPersistentStorage(context).getPersistentID();
		}
		
		@Override
		public DistributionModel getDistributionModel() {
			return table.getDistributionVector(context).getModel().getSingleton();
		}

		@Override
		public LinkedHashMap<String, ? extends IColumnDatum> getValues() { //NOPMD
			if (externalValues == null) {
				externalValues = new LinkedHashMap<String,TColumnDatum>();
				for(PEColumn c : columnOrder) {
					TColumnDatum tcd = values.get(c);
					if (tcd != null) externalValues.put(
							tcd.getColumn().getPersistentName(),tcd);
				}
			}
			return externalValues;
		}

		@Override
		public int hashCode() {
			return KeyValue.buildHashCode(this);
		}

		@Override
		public boolean equals(Object o) {
			return KeyValue.equals((IKeyValue)this,(IKeyValue)o);
		}

		@Override
		public DistributionModel getContainerDistributionModel() {
			if (table.getDistributionVector(context).getModel() != Model.CONTAINER)
				return null;
			return table.getDistributionVector(context).getContainer(context).getContainerDistributionModel().getSingleton();
		}

		@Override
		public String toString() {
			return PEStringUtils.toString(table.getName(context).getUnquotedName().get(), getValues());
		}

		@Override
		public Integer getRangeId() {
			return table.getDistributionVector(context).getRangeID(context);
		}
		
	}
}
