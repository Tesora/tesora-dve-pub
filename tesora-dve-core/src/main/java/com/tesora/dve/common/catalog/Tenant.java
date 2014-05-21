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

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

@InfoSchemaTable(logicalName="tenant",
		views={@TableView(view=InfoView.SHOW, name="tenant", pluralName="tenants", 
				columnOrder={ShowSchema.Tenant.NAME, 
							 ShowSchema.Tenant.DATABASE, 
							 ShowSchema.Tenant.SUSPENDED, 
							 ShowSchema.Tenant.DESCRIPTION}, extension=true, priviledged=true),
		       @TableView(view=InfoView.INFORMATION, name="tenant", pluralName="", columnOrder={"name", "database", "suspended", "description", "id"}, extension=true, priviledged=true)})
@Entity
@Table(name="tenant")
public class Tenant implements ITenant {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name="tenant_id")
	int id;
	
	@Column(name="ext_tenant_id", unique=true,nullable=false)
	String externalID;
	
	@Column(name="description")
	String description;
	
	@Column(name="suspended")
	Boolean suspended;
	
	@OneToMany(fetch=FetchType.LAZY)
	@JoinColumn(name="scope_tenant_id")
	Set<TableVisibility> visibility;
	
	// this is a tenant on a database - ref the database
	@ForeignKey(name="fk_tenant_db")
	@ManyToOne(optional = false)
	@JoinColumn(name = "user_database_id")
	UserDatabase userDatabase;
		
	private transient ColumnSet showColumnSet = null;

	public Tenant(UserDatabase onDB, String extid, String description) {
		this.externalID = extid;
		this.description = description;
		this.suspended = Boolean.FALSE;
		this.userDatabase = onDB;
	}
	
	public Tenant() {
		
	}

	@Override
	@InfoSchemaColumn(logicalName="id", fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={@ColumnView(view=InfoView.INFORMATION,name="id")})
	public int getId() {
		return id;
	}
	
	@InfoSchemaColumn(logicalName="name",fieldName="externalID",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Tenant.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="name", orderBy=true, ident=true)})
	public String getExternalTenantId() {
		return externalID;
	}
	
	@InfoSchemaColumn(logicalName="description", fieldName="description",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Tenant.DESCRIPTION),
				   @ColumnView(view=InfoView.INFORMATION,name="description")})
	public String getDescription() {
		return description;
	}

	@InfoSchemaColumn(logicalName="suspended", fieldName="suspended",
			sqlType=java.sql.Types.VARCHAR,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Tenant.SUSPENDED),
				   @ColumnView(view=InfoView.INFORMATION,name="suspended")})
	public Boolean isSuspended() {
		return suspended;
	}
	
	@InfoSchemaColumn(logicalName="database",fieldName="userDatabase",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Tenant.DATABASE),
				   @ColumnView(view=InfoView.INFORMATION,name="database")})
	public UserDatabase getDatabase() {
		return userDatabase;
	}
	
	public void setSuspended() {
		suspended = true;
	}
	
	public void setResumed() {
		suspended = false;
	}

	public Set<TableVisibility> getScoping() {
		return visibility;
	}
	
	public List<UserTable> getVisibleTables() {
		return Functional.apply(visibility, new UnaryFunction<UserTable, TableVisibility>() {

			@Override
			public UserTable evaluate(TableVisibility object) {
				return object.getTable();
			}
			
		});
	}
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Name", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Suspended", 8, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Description", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("ID", 8, "int", Types.INTEGER);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.externalID, false);
		rr.addResultColumn(this.suspended ? "yes" : "no", false);
		rr.addResultColumn(this.description, false);
		rr.addResultColumn(this.id, false);
		return rr;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		ArrayList out = new ArrayList();
		for(TableVisibility tv : getScoping()) {
			out.addAll(tv.getDependentEntities(c));
			out.add(tv);
		}
		// also, we need to drop the privs for any user that references this tenant
		out.addAll(c.findPriviledgesOnTenant(this));
		return out;
	}

	@Override
	public String toString() {
		return "Tenant ID: " + id + " Name: " + externalID;
	}

	@Override
	public String getUniqueIdentifier() {
		return externalID;
	}

	@Override
	public boolean isGlobalTenant() {
		return PEConstants.LANDLORD_TENANT.equals(externalID);
	}

	@Override
	public boolean isPersistent() {
		// always true - the landlord tenant is an actual tenant
		return true;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
}
