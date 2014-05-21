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

import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;

// the purpose of a priviledge is to tie a user to one or more non-info-schema databases or tenants

@InfoSchemaTable(logicalName="priviledge",views={})
@Entity
@Table(name="priviledge")
public class Priviledge implements CatalogEntity {

	@Id
	@GeneratedValue
	@Column( name="id" )
	int id;

	// the user this is a priviledge for
	@ForeignKey(name="fk_priv_user")
	@ManyToOne()
	@JoinColumn(name="user_id")
	User user;

	// if this is a priviledge for a user database
	@ForeignKey(name="fk_priv_db")
	@ManyToOne(optional = true)
	@JoinColumn(name="user_database_id")
	UserDatabase database;
	
	// if this is a priviledge for a tenant
	@ForeignKey(name="fk_priv_tenant")
	@ManyToOne(optional = true)
	@JoinColumn(name="tenant_id")
	Tenant tenant;
	
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private Priviledge() {
		
	}
	
	public Priviledge(User forUser, UserDatabase udb) {
		user = forUser;
		database = udb;
		tenant = null;
	}
	
	public Priviledge(User forUser, Tenant forTenant) {
		user = forUser;
		database = null;
		tenant = forTenant;
	}
	
	// special priviledge - basically means global access
	// i.e. grant all on *.*
	public Priviledge(User forUser) {
		user = forUser;
		database = null;
		tenant = null;
	}
	
	@Override
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName="tenant", fieldName="tenant",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public Tenant getTenant() {
		return tenant;
	}
	
	@InfoSchemaColumn(logicalName="database", fieldName="database",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public UserDatabase getDatabase() {
		return database;
	}
	
	@InfoSchemaColumn(logicalName="user", fieldName="user",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public User getUser() {
		return user;
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

	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		// dropping a priviledge does not drop anything else
		return Collections.emptyList();
	}

	public boolean matches(UserDatabase udb, Tenant ten) {
		if (udb != null && database != null && udb.getId() == database.getId()) return true;
		if (ten != null && tenant != null && ten.getId() == tenant.getId()) return true;
		if (udb == null && database == null && ten == null && tenant == null) return true;
		return false;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
