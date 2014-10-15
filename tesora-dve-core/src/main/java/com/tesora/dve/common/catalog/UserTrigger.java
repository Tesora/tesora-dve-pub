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
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name = "user_trigger")
public class UserTrigger implements CatalogEntity {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "trigger_id")
	int id;

	@Column(name = "trigger_name", nullable = false)
	String name;
	
	@Column(name = "trigger_body", nullable = false)
	@Lob
	String definition;

	// somewhat redundant but oh well
	@Column(name = "origsql", nullable = false)
	@Lob
	String origsql;
	
	@ForeignKey(name="fk_trigger_user")
	@ManyToOne(optional = false)
	@JoinColumn(name = "user_id")
	User definer;
	
	// this is the target table - the one upon which updates cause the trigger to fire
	@ForeignKey(name="fk_trigger_table_def")
	@ManyToOne(optional=false,fetch=FetchType.LAZY)
	@JoinColumn(name="table_id")
	UserTable table;

	// i.e. INSERT, UPDATE, DELETE
	@Column(name = "trigger_event", nullable = false)
	String event;
	
	// i.e. BEFORE, AFTER
	@Column(name = "trigger_time", nullable = false)
	String when;
	
	@Column(name = "sql_mode", nullable = false)
	@Lob
	String sqlMode;
	
	@Column(name = "character_set_client", nullable = false)
	String charset;
	
	@Column(name = "collation_connection", nullable = false)
	String collation;
	
	@Column(name = "database_collation", nullable = false)
	String databaseCollation;

	public UserTrigger() {
		
	}
	
	public UserTrigger(String name, String body, UserTable triggerTable, 
			String event, String when, String sqlMode, 
			String charSetClient, String collationConnection, String databaseCollation, 
			User definer, String origsql) {
		this.name = name;
		this.definition = body;
		this.table = triggerTable;
		triggerTable.getTriggers().add(this);
		this.event = event;
		this.when = when;
		this.sqlMode = sqlMode;
		this.charset = charSetClient;
		this.collation = collationConnection;
		this.databaseCollation = databaseCollation;
		this.definer = definer;
		this.origsql = origsql;
	}
	
	@Override
	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}
	
	public String getBody() {
		return definition;
	}
	
	public UserTable getTable() {
		return table;
	}
	
	public String getEvent() {
		return event;
	}
	
	public String getWhen() {
		return when;
	}
	
	public String getCharsetConnection() {
		return charset;
	}
	
	public String getCollationConnection() {
		return collation;
	}
	
	public String getDatabaseCollation() {
		return databaseCollation;
	}
	
	public String getSQLMode() {
		return sqlMode;
	}
	
	public User getDefiner() {
		return definer;
	}
	
	public String getOrigSQL() {
		return origsql;
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
		return Collections.EMPTY_LIST;
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
