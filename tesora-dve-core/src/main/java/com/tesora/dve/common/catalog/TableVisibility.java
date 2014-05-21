// OS_STATUS: public
package com.tesora.dve.common.catalog;

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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Index;
import org.hibernate.annotations.ForeignKey;


import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;

@InfoSchemaTable(logicalName="table_visibility", 
	views={})
@Entity
@Table(name="scope", uniqueConstraints=@UniqueConstraint(columnNames = {"scope_tenant_id", "local_name" }))
public class TableVisibility implements CatalogEntity, HasAutoIncrementTracker {

	private static final long serialVersionUID = 1L;
	
	@Id
	@GeneratedValue
	@Column(name="scope_id")
	int id;

	@ForeignKey(name="fk_scope_tenant")
	@ManyToOne()
	@JoinColumn(name="scope_tenant_id")
	Tenant ofTenant;
	
	@ForeignKey(name="fk_scope_table")
	@ManyToOne()
	@JoinColumn(name="scope_table_id")
	UserTable table;

	@OneToOne(mappedBy="tenantTable", cascade=CascadeType.ALL, optional=true)
	AutoIncrementTracker autoIncr;
	
	@Index(name="local_name_idx")
	@Column(name="local_name",nullable=false)
	String localName;
	
	public TableVisibility() {
		
	}
	
	public TableVisibility(Tenant forTenant, UserTable tab, String name, Long autoIncrStart) {
		ofTenant = forTenant;
		setTable(tab);
		this.localName = name;
		if (tab.hasAutoIncr()) {
			autoIncr = new AutoIncrementTracker(this, autoIncrStart);
		}
	}

	@InfoSchemaColumn(logicalName="user_table", fieldName="table",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public UserTable getTable() {
		return table;
	}

	public void setTable(UserTable target) {
		table = target;
	}
	
	@InfoSchemaColumn(logicalName="tenant", fieldName="ofTenant",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public Tenant getTenant() {
		return ofTenant;
	}

	// null if the backing table name should be used
	@InfoSchemaColumn(logicalName="local_name", fieldName="localName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getLocalName() {
		return this.localName;
	}
	
	public String getName() {
		return (localName != null ? localName : table.getName());
	}
	
	@Override
	@InfoSchemaColumn(logicalName="id", fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={})
	public int getId() {
		return id;
	}
	
	@Override
	public boolean hasAutoIncr() {
		return autoIncr != null;
	}
		
	public AutoIncrementTracker getAutoIncr() {
		return autoIncr;
	}
		
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		ofTenant.getScoping().remove(this);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		ArrayList out = new ArrayList();
		if (autoIncr != null) {
			out.addAll(autoIncr.getDependentEntities(c));
			out.add(autoIncr);
		}
		// don't add the table here - the table will be handled elsewhere or by
		// the garbage collector
		return out;
	}

	@Override
	public String toString() {
		return "Scope: ID: " + id + " Tenant: " + ofTenant.getId() + " Table: " + (table == null ? null : table.getId()); 
	}

	@Override
	public long getNextIncrValue(CatalogDAO c) {
		return autoIncr.getNextValue(c);
	}

	@Override
	public long getNextIncrBlock(CatalogDAO c, long blockSize) {
		return autoIncr.getIdBlock(c, blockSize);
	}
	
	@Override
	public void removeNextIncrValue(CatalogDAO c, long value) {
		autoIncr.removeValue(c, value);
	}
	
	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
