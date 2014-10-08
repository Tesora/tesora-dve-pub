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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="character_sets")
public class CharacterSets implements CatalogEntity {

	private static final long serialVersionUID = 1L;
	
	@Id
	@Column(name = "id",columnDefinition="integer(11)")
	int id;

	@Column(name="character_set_name",columnDefinition="varchar(32) default ''",nullable=false)
	String characterSetName;

	@Column(name="description",columnDefinition="varchar(60) default ''",nullable=false)
	String description;
	
	@Column(name="maxlen",columnDefinition="bigint(3) default '0'",nullable=false)
	long maxlen;

	@Column(name="pe_character_set_name",columnDefinition="varchar(32) default ''",nullable=false)
	String peCharacterSetName;

	public CharacterSets(int id, String characterSetName, String description, long maxlen, String pe_character_set_name) {
		this.id = id;
		this.characterSetName = characterSetName;
		this.description = description;
		this.maxlen = maxlen;
		this.peCharacterSetName = pe_character_set_name;
	}

	public CharacterSets() {
	}
	
	@Override
	public int getId() {
		return id;
	}

	public String getCharacterSetName() {
		return characterSetName;
	}

	public String getDescription() {
		return description;
	}

	public long getMaxlen() {
		return maxlen;
	}

	public String getPeCharacterSetName() {
		return peCharacterSetName;
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
