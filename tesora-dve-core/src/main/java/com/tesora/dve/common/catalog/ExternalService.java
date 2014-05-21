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
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName="external_service",
 views = {
		@TableView(view = InfoView.SHOW, name = "external service", pluralName = "external services", columnOrder = {
				ShowSchema.ExternalService.NAME, 
				ShowSchema.ExternalService.PLUGIN, 
				ShowSchema.ExternalService.AUTO_START, 
				ShowSchema.ExternalService.CONNECT_USER, 
				ShowSchema.ExternalService.USES_DATASTORE }, extension = true, priviledged = true),
		@TableView(view = InfoView.INFORMATION, name = "external_service", pluralName = "", columnOrder = {
				"name", "plugin", "auto_start", "connect_user", "uses_datastore" }, extension = true, priviledged = true) })
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

	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name="name",orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="name", orderBy=true, ident=true)})
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	@InfoSchemaColumn(logicalName="plugin",fieldName="plugin",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name="plugin"),
			       @ColumnView(view=InfoView.INFORMATION, name="plugin")})
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

	@InfoSchemaColumn(logicalName="connect user",fieldName="connectUser",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name="connect_user"),
			       @ColumnView(view=InfoView.INFORMATION, name="connect_user")})
	public String getConnectUser() {
		return connectUser;
	}

	public void setConnectUser(String connectUser) {
		this.connectUser = connectUser;
	}

	@InfoSchemaColumn(logicalName="auto_start",fieldName="autoStart",
			sqlType=java.sql.Types.VARCHAR,
			views={@ColumnView(view=InfoView.SHOW, name="auto_start"),
			       @ColumnView(view=InfoView.INFORMATION, name="auto_start")})
	public Boolean isAutoStart() {
		return autoStart;
	}

	public void setAutoStart(Boolean autoStart) {
		this.autoStart = autoStart;
	}

	@InfoSchemaColumn(logicalName="uses_datastore",fieldName="usesDataStore",
			sqlType=java.sql.Types.VARCHAR,
			views={@ColumnView(view=InfoView.SHOW, name="uses_datastore"),
			       @ColumnView(view=InfoView.INFORMATION, name="uses_datastore")})
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
