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
import javax.persistence.Lob;
import javax.persistence.Table;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="template")
public class PersistentTemplate implements CatalogEntity, NamedCatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "template_id")
	private int id;

	@Column(name = "name", nullable = false)
	private String name;
	
	@Column(name = "definition", nullable = false)
	@Lob
	String definition;

	@Column(name = "dbmatch", nullable = true)
	String dbmatch;
	
	@Column(name = "template_comment", nullable = true)
	String comment;
	
	@Override
	public int getId() {
		// TODO Auto-generated method stub
		return id;
	}

	public PersistentTemplate() {
		
	}
	
	public PersistentTemplate(String name, String def, String match, String comment) {
		this.name = name;
		this.definition = def;
		this.dbmatch = match;
		this.comment = comment;
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

	
	public String getMatch() {
		return dbmatch;
	}
	
	public void setMatch(String def) {
		dbmatch = def;
	}

	public String getComment() {
		return comment;
	}
	
	public void setComment(String v) {
		comment = v;
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
