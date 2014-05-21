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


import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;

@InfoSchemaTable(logicalName="views",views={})
@Entity
@Table(name = "user_view")
public class UserView implements CatalogEntity {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "view_id")
	int id;

	@Column(name = "definition", nullable = false)
	@Lob
	String definition;

	@ForeignKey(name="fk_view_user")
	@ManyToOne(optional = false)
	@JoinColumn(name = "user_id")
	User definer;

	// this is the table definition that looks like this view
	@ForeignKey(name="fk_view_table_def")
	@OneToOne(optional=false,fetch=FetchType.LAZY)
	@JoinColumn(name="table_id")
	UserTable table;

	@Column(name = "character_set_client", nullable = false)
	String charset;
	
	@Column(name = "collation_connection", nullable = false)
	String collation;
	
	// dve specific - true if we push this view down to the persistent sites
	// this is a ViewMode in real life
	@Column(name = "mode", nullable = false)
	String mode;
	
	@Column(name = "security", nullable = false)
	String security;
	
	@Column(name = "algorithm", nullable = false)
	String algorithm;
	
	@Column(name = "check_option", nullable = false)
	String check;
	
	public UserView() {
		
	}
	
	
	public UserView(User definer, String sql, 
			String charset, String collation, 
			ViewMode vm,
			String security, String algo, String checkOption) {
		this.definer = definer;
		this.definition = sql;
		this.charset = charset;
		this.collation = collation;
		this.mode = vm.getPersistentValue();
		this.security = security;
		this.algorithm = algo;
		this.check = checkOption;
	}

	@Override
	public int getId() {
		return id;
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
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

	@InfoSchemaColumn(logicalName="definition",fieldName="definition",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255, views={})
	public String getDefinition() {
		return definition;
	}

	@InfoSchemaColumn(logicalName="definedby", fieldName="definer",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})			
	public User getDefiner() {
		return definer;
	}

	public void setTable(UserTable ut) {
		table = ut;
	}
	
	@InfoSchemaColumn(logicalName="backing",fieldName="table",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public UserTable getTable() {
		return table;
	}

	@InfoSchemaColumn(logicalName="client_charset",fieldName="charset",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=32,
			views={})
	public String getCharset() {
		return charset;
	}
	
	@InfoSchemaColumn(logicalName="connection_collation",fieldName="collation",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=32,
			views={})
	public String getCollation() {
		return collation;
	}

	public ViewMode getViewMode() {
		return ViewMode.toMode(mode);
	}
	
	@InfoSchemaColumn(logicalName="viewmode",fieldName="mode",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getMode() {
		return mode;
	}
	
	@InfoSchemaColumn(logicalName="security",fieldName="security",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getSecurity() {
		return security;
	}

	@InfoSchemaColumn(logicalName="algorithm",fieldName="algorithm",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getAlgorithm() {
		return algorithm;
	}

	@InfoSchemaColumn(logicalName="check",fieldName="check",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getCheckOption() {
		return check;
	}

}
