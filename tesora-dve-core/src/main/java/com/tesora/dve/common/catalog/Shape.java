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
@Table(name = "shape")
public class Shape implements CatalogEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name="shape_id")
	int id;

	// this is the logical name, not the mangled name 
	@Column(name="name", nullable=false)
	String name;

	// we need a unique key on {name,definition}, but since definition is longtext
	// we can't get one.  instead we also store a sha1 hash of the definition.  this is
	// 40 characters long - which is short enough to add the unique key.  although
	// the hash is unlikely to have collisions, adding the name into the key will
	// almost guarantee this to be the case.
	@Column(name="typehash", nullable=false, length=40)
	String typeHash;
	
	@Column(name = "definition", nullable = false)
	@Lob
	String tableDefinition;

	@ForeignKey(name="fk_shape_db")
	@ManyToOne(optional = false)
	@JoinColumn(name = "database_id")
	UserDatabase userDatabase;

	@Override
	public int getId() {
		return id;
	}

	public Shape() {
		
	}
	
	public Shape(UserDatabase onDB, String logicalName, String tableDefinition, String typeHash) {
		userDatabase = onDB;
		name = logicalName;
		this.tableDefinition = tableDefinition;
		this.typeHash = typeHash;
	}
	
	public String getTableDefinition() {
		return this.tableDefinition;
	}
	
	public String getName() {
		return name;
	}
	
	public String getTypeHash() {
		return typeHash;
	}
	
	public UserDatabase getDatabase() {
		return userDatabase;
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
		// never directly deleted
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
