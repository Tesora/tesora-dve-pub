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
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name = "user_key_column")
public class KeyColumn implements CatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "key_column_id")
	int id;

	@ForeignKey(name="fk_key_column_src_column")
	@ManyToOne(optional = false)
	@JoinColumn(name = "src_column_id")
	private UserColumn column;

	@Column(name = "position", nullable = false)
	private int position;

	// for columns that require a prefix length - that length
	@Column(name = "length", nullable = true)
	private Integer length;
	
	// if a foreign key, the target key
	@ForeignKey(name="fk_key_column_targ_column")
	@ManyToOne(optional = true)
	@JoinColumn(name = "targ_column_id")
	private UserColumn targetColumn;
	
	// if one of those fancy forward ref fks, the forward table name and column
	@Column(name="forward_column_name", nullable=true)
	private String targetColumnName;
	
	
	@ForeignKey(name="fk_key_column_key")
	@ManyToOne(optional = false)
	@JoinColumn(name = "key_id", nullable = false)
	private Key key;
	
	// we don't set cardinality for foreign keys
	@Column(name="cardinality", nullable=true)
	Long cardinality;
	
	public KeyColumn() {
		
	}
	
	// position here is 1 based
	public KeyColumn(UserColumn col, Integer len, int pos, Long card) {
		column = col;
		position = pos;
		length = len;
		targetColumn = null;
		targetColumnName = null;
		this.cardinality = card;
	}

	public KeyColumn(UserColumn col, int pos, UserColumn refCol) {
		this(col,null,pos, null);
		targetColumn = refCol;
		targetColumnName = null;
	}

	public KeyColumn(UserColumn col, int pos, String targetColumnName) {
		this(col, null, pos, null);
		this.targetColumn = null;
		this.targetColumnName = targetColumnName;
	}
	
	@Override
	public int getId() {
		return id;
	}

	public void setTargetColumn(UserColumn uc) {
		targetColumn = uc;
		if (targetColumn != null) 
			targetColumnName = null;
	}
	
	public void setTargetColumn(String name) {
		if (name != null) {
			targetColumn = null;
			targetColumnName = name;
		}
	}
	
	public UserColumn getSourceColumn() {
		return column;
	}

	public int getPosition() {
		return position;
	}

	public UserColumn getTargetColumn() {
		return targetColumn;
	}
	
	public void setKey(Key k) {
		key = k;
	}
	
	public Key getKey() {
		return key;
	}
		
	public String getTargetColumnName() {
		if (targetColumn != null) return targetColumn.getName();
		return targetColumnName;
	}
	
	public Integer getLength() {
		return length;
	}
	
	public void setLength(Integer l) {
		length = l;
	}

	public Long getCardinality() {
		return cardinality;
	}

	public void setCardinality(Long v) {
		cardinality = v;
	}
	
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		throw new PEException("Unused");
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		throw new PEException("Unused");
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
