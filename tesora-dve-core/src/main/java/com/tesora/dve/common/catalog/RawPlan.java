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
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="rawplan")
public class RawPlan implements CatalogEntity, NamedCatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "plan_id")
	private int id;

	@Column(name = "name", nullable = false)
	private String name;
	
	@Column(name = "definition", nullable = false)
	@Lob
	String definition;

	@Column(name = "plan_comment", nullable = true)
	String comment;
	
	@ForeignKey(name="fk_raw_plan_db")
	@ManyToOne(optional = false)
	@JoinColumn(name = "user_database_id")
	UserDatabase userDatabase;
	
	@Column(name = "enabled", nullable = false)
	int enabled;
	
	// this is the shrunk value
	@Column(name = "cachekey", nullable = false)
	@Lob
	String cacheKey;
	
	@Override
	public int getId() {
		return id;
	}

	public RawPlan() {
		
	}
	
	public RawPlan(String name, String def, UserDatabase ofDB, boolean enabled, String shrunk, String comment) {
		this.name = name;
		this.definition = def;
		this.comment = comment;
		this.userDatabase = ofDB;
		this.enabled = (enabled ? 1 : 0);
		this.cacheKey = shrunk;
	}
	
	@Override
	public String getName() {
		return name;
	}

	public String getDefinition() {
		return definition;
	}
	
	public void setDefinition(String def) {
		definition = def;
	}

	public String getCacheKey() {
		return cacheKey;
	}
	
	public void setCacheKey(String def) {
		cacheKey = def;
	}

	public String getComment() {
		return comment;
	}
	
	public void setComment(String v) {
		comment = v;
	}
	
	public UserDatabase getDatabase() {
		return userDatabase;
	}

	public void setDatabase(UserDatabase database) {
		userDatabase = database;
	}
	
	public boolean isEnabled() {
		return (enabled == 1 ? true : false);
	}
	
	public void setEnabled(boolean v) {
		enabled = (v ? 1 : 0);
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

}
