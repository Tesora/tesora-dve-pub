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
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="external_service")
public class ExternalService implements CatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column( name="id" )
	int id;

	@Column(name="name", unique=true, nullable=false)
	String name;
 
	@Column(name="plugin", nullable=false)
	String plugin;
	
	@Column(name="config", nullable=true)
	@Lob
	String config;
	
	@Column(name="connect_user", nullable=true)
	String connectUser;
	
	@Column(name="auto_start", nullable=false)
	Boolean autoStart;

	@Column(name="uses_datastore", nullable=false)
	Boolean usesDataStore;

	private transient ColumnSet showColumnSet = null;

	public ExternalService(String name, String plugin, String connectUser, boolean usesDataStore) {
		this(name, plugin, connectUser, usesDataStore, null);
	}

	public ExternalService(String name, String plugin, String connectUser, boolean usesDataStore, String config) {
		this.name = name;
		this.plugin = plugin;
		this.connectUser = connectUser;
		this.usesDataStore = usesDataStore;
		this.config = config;
		setAutoStart(true);
	}

	public ExternalService() {
		setAutoStart(true);
		setUsesDataStore(false);
	}

	@Override
	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public String getPlugin() {
		return plugin;
	}
	
	public void setPlugin(String plugin) {
		this.plugin = plugin;
	}

	public String getConfig() {
		return config;
	}
	
	public void setConfig(String config) {
		this.config = config;
	}

	public String getConnectUser() {
		return connectUser;
	}

	public void setConnectUser(String connectUser) {
		this.connectUser = connectUser;
	}

	public Boolean isAutoStart() {
		return autoStart;
	}

	public void setAutoStart(Boolean autoStart) {
		this.autoStart = autoStart;
	}

	public Boolean usesDataStore() {
		return usesDataStore;
	}

	public void setUsesDataStore(Boolean usesDataStore) {
		this.usesDataStore = usesDataStore;
	}

	public String getDataStoreName() {
		String rv = null;
		if ( usesDataStore() ) {
			rv = "es" + new Integer(getId()).toString() + "_" + getName();
		}
		
		return rv;
	}
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) throws PEException {
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Name", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Plugin", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Auto Start", 3, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Uses DataStore", 3, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Connect User", 32, "varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) throws PEException {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
		rr.addResultColumn(this.plugin, false);
		rr.addResultColumn(isAutoStart() ? "YES" : "NO");
		rr.addResultColumn(usesDataStore() ? "YES" : "NO");
		rr.addResultColumn(this.connectUser);
		return rr;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
