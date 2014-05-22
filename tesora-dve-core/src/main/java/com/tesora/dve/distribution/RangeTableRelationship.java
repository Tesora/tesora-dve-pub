package com.tesora.dve.distribution;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.catalog.CachedCatalogLookup;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;

@InfoSchemaTable(logicalName="range_table_relation",views={})
@Entity
@Table(name="range_table_relation" , uniqueConstraints=@UniqueConstraint(columnNames = { "table_id" }))
public class RangeTableRelationship implements CatalogEntity {

	private static final long serialVersionUID = 1L;
	
	@Id
	@GeneratedValue
	@Column(name = "relationship_id")
	private int id;

	@ForeignKey(name="fk_range_table_table")
	@OneToOne(optional=false)
	@JoinColumn(name = "table_id")
	UserTable table;
	
	@ForeignKey(name="fk_range_table_range")
	@ManyToOne(optional=false)
	@JoinColumn(name = "range_id")
	DistributionRange distributionRange;
	
	public RangeTableRelationship() {
	}
	
	public RangeTableRelationship(UserTable table, DistributionRange range) {
		this.table = table;
		this.distributionRange = range;
	}
	
	@InfoSchemaColumn(logicalName="user_table", fieldName="table",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public UserTable getTable() {
		return table;
	}

	@InfoSchemaColumn(logicalName="distribution_range", fieldName="distributionRange",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public DistributionRange getRange() {
		return distributionRange;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeFromParent() {
		// TODO Actually implement the removal of this instance from the parent
	}
	
	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// TODO Return a valid list of dependents
		return Collections.emptyList();
	}
	
	@Override
	public String toString() {
		return "RangeTableRelationship on Table id: " + table.getId() + "; range: " + distributionRange.toString();
	}
	
	@Override
	public int getId() {
		return id;
	}
	
	public static class RangeTableRelationshipCacheLookup extends CachedCatalogLookup<RangeTableRelationship> {

		// we use two different column names
		
		public RangeTableRelationshipCacheLookup(
				Class<? extends CatalogEntity> tableClass) {
			super(tableClass, "unused");
		}
		
		@Override
		public RangeTableRelationship queryByValue(CatalogDAO c, Object value, Object param, boolean exceptionOnNotFound) throws PENotFoundException {
			RangeTableRelationship instance;
			String prefix = "from RangeTableRelationship where ";
			String columnName = null;
			if (param instanceof UserTable)
				columnName = "table";
			else
				columnName = "table.id";
			String queryString = prefix + columnName + " = :value";
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("value", param);
			List<CatalogEntity> res = c.queryCatalogEntity(queryString, params);
			instance = (RangeTableRelationship) CatalogDAO.onlyOne(res, "RangeTableRelationship", value.toString(), exceptionOnNotFound);
			if (instance != null)
				valueLookup.put(value, instance.getId());
			return instance;
		}

		
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
