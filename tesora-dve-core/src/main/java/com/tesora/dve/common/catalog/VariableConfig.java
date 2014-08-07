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
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.TableView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.ColumnView;

@InfoSchemaTable(logicalName="variable_defs",
	views={@TableView(view=InfoView.INFORMATION, name="variable_definitions", pluralName="",
		        columnOrder={"name","value","type","scopes","options","description"})})
@Entity
@Table(name="varconfig", uniqueConstraints=@UniqueConstraint(columnNames = { "name" }))
public class VariableConfig implements CatalogEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column( name="id" )
	int id;

	@Column(name="name",nullable=false)
	String name;
	
	@Column(name="value_type",nullable=false)
	String valueType;

	// null is a valid value
	@Column(name="value",nullable=true)
	String value;
	
	// i.e. SESSION,GLOBAL
	@Column(name="scopes",nullable=false)
	String scopes;
	
	// a brief description
	@Column(name="description",nullable=true)
	String help;
	
	// 1 for emulated, 0 for not (i.e. dve only)
	@Column(name="options",nullable=false)
	String options;
	
	public VariableConfig() {
		
	}
	
	public VariableConfig(String name, String valueType, String value, String scopes, String options, String helpText) {
		this.name = name;
		this.valueType = valueType;
		this.value = value;
		this.scopes = scopes;
		this.options = options;
		this.help = helpText;
	}
	
	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="name",orderBy=true,ident=true)})
	public String getName() {
		return name;
	}
	
	@InfoSchemaColumn(logicalName="type",fieldName="valueType",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="type",orderBy=false,ident=false)})	
	public String getValueType() {
		return valueType;
	}

	@InfoSchemaColumn(logicalName="value",fieldName="value",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="value",orderBy=false,ident=false)})	
	public String getValue() {
		return value;
	}
	
	public void setValue(String v) {
		value = v;
	}

	@InfoSchemaColumn(logicalName="scopes",fieldName="scopes",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="scopes",orderBy=false,ident=false)})	
	public String getScopes() {
		return scopes;
	}
	
	@InfoSchemaColumn(logicalName="options", fieldName="options",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="options",orderBy=false,ident=false)})
	public String getOptions() {
		return options;
	}

	@InfoSchemaColumn(logicalName="description",fieldName="help",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="description",orderBy=false,ident=false)})	
	public String getHelp() {
		return help;
	}

	@Override
	public int getId() {
		return id;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDrop() {
		// TODO Auto-generated method stub
		
	}
}
