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

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang.BooleanUtils;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="collations")
public class Collations implements CatalogEntity {

	private static final long serialVersionUID = 1L;
	
	@Column(name="name",columnDefinition="varchar(32)",nullable=false)
	String name;
	
	@Column(name="character_set_name",columnDefinition="varchar(32)",nullable=false)
	String characterSetName;

	@Id
	@Column(name = "id", columnDefinition = "int(11)", nullable = false)
	int id;

	@Column(name="is_default",columnDefinition="int(11) default '0'",nullable=false)
	int isDefault;
	
	@Column(name="is_compiled",columnDefinition="int(11) default '1'",nullable=false)
	int isCompiled;

	@Column(name="sortlen",columnDefinition="bigint(3)",nullable=false)
	long sortlen;

	public Collations(String name, String characterSetName, int id, boolean isDefault, boolean isCompiled, long sortlen) {
		this.name = name;
		this.characterSetName = characterSetName;
		this.id = id;
		this.isDefault = BooleanUtils.toInteger(isDefault);
		this.isCompiled = BooleanUtils.toInteger(isCompiled);
		this.sortlen = sortlen;
	}

	public Collations() {
	}
	
	public String getName() {
		return name;
	}

	public String getCharacterSetName() {
		return characterSetName;
	}
	
	public long getCollationId() {
		return id;
	}

	@Override
	public int getId() {
		return id;
	}

	public boolean getIsDefault() {
		return BooleanUtils.toBoolean(isDefault);
	}

	public boolean getIsCompiled() {
		return BooleanUtils.toBoolean(isCompiled);
	}

	public long getSortlen() {
		return sortlen;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		// no show
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		// no show
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// no parents
		
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		// no dependents
		return null;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
}
