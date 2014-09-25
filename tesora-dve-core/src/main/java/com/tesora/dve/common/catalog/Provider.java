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
@Table(name="provider")
public class Provider implements CatalogEntity {

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
	
	@Column(name="enabled", nullable=false)
	Boolean isEnabled;
	
	private transient ColumnSet showColumnSet = null;

	public Provider(String name, String plugin) {
		this(name, plugin, null);
	}

	public Provider(String name, String plugin, String config) {
		this.name = name;
		this.plugin = plugin;
		this.config = config;
		setIsEnabled(true);
	}

	public Provider() {
		setIsEnabled(true);
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

	public Boolean isEnabled() {
		return isEnabled;
	}

	public void setIsEnabled(Boolean isEnabled) {
		this.isEnabled = isEnabled;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) throws PEException {
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Name", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Plug-in", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Enabled", 3, "varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) throws PEException {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
		rr.addResultColumn(this.plugin, false);
		rr.addResultColumn(isEnabled() ? "YES" : "NO");
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
