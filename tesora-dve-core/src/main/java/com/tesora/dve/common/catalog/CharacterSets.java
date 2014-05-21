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

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName = "character_sets", views = {
		@TableView(view = InfoView.SHOW, name = "", pluralName = "charset", 
				columnOrder = { ShowSchema.CharSet.CHARSET,
								ShowSchema.CharSet.DESCRIPTION,
								ShowSchema.CharSet.MAXLEN }),
		@TableView(view = InfoView.INFORMATION, name = "character_sets", pluralName = "", columnOrder = {
				"character_set_name", "description", "maxlen"
		}) })
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

	@InfoSchemaColumn(logicalName = "character set name", fieldName = "characterSetName", sqlType = java.sql.Types.VARCHAR, sqlWidth = 32, 
			views = {@ColumnView(view = InfoView.SHOW, name = ShowSchema.CharSet.CHARSET, orderBy = true, ident = true),
					 @ColumnView(view = InfoView.INFORMATION, name = "character_set_name", orderBy = true, ident = true) })
	public String getCharacterSetName() {
		return characterSetName;
	}

	@InfoSchemaColumn(logicalName="description", fieldName="description",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=60,
			views={@ColumnView(view=InfoView.SHOW, name = ShowSchema.CharSet.DESCRIPTION),
				   @ColumnView(view=InfoView.INFORMATION, name="description")})
	public String getDescription() {
		return description;
	}

	@InfoSchemaColumn(logicalName="maxlen", fieldName="maxlen",
			sqlType=java.sql.Types.INTEGER,
			views={@ColumnView(view=InfoView.SHOW, name = ShowSchema.CharSet.MAXLEN),
				   @ColumnView(view=InfoView.INFORMATION, name="maxlen")})
	public long getMaxlen() {
		return maxlen;
	}

//	@InfoSchemaColumn(logicalName="pe character set name", fieldName="peCharacterSetName",
//			sqlType=java.sql.Types.VARCHAR,sqlWidth=32,
//			views={@ColumnView(view=InfoView.INFORMATION, name="pe_character_set_name")})
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
