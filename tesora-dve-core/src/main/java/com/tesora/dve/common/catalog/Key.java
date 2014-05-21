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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;

@InfoSchemaTable(logicalName="key",views={})
@Entity
@Table(name = "user_key")
public class Key implements CatalogEntity {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name="key_id")
	private int id;

	@Column(name="name",nullable=false)
	private String name;
	
	@Column(name = "index_type", nullable = false)
	@Enumerated(EnumType.STRING)
	private IndexType type;
	
	@Column(name="constraint_name",nullable=true)
	private String constraintName;
	
	// used in mt mode when fks are present.
	@Column(name="physical_constraint_name",nullable=true)
	private String physicalSymbol;
	
	@Column(name = "constraint_type", nullable = true)
	@Enumerated(EnumType.STRING)
	private ConstraintType constraint;
	
	// this is the order within the keys section of the table decl of this key
	// we need to store this even though it is mostly set by us - the reason being that the unpersisted keys are not put in
	// the create table stmt and that used to be the only place order is kept
	@Column(name = "position", nullable=false)
	private int position;
	
	// why eager you ask?  well, most of the time if we care about a key, we want to know the columns
	@OneToMany(cascade=CascadeType.ALL, mappedBy="key", fetch=FetchType.EAGER)
	private List<KeyColumn> columns = new ArrayList<KeyColumn>();
	
	@ForeignKey(name="fk_key_src_table")
	@ManyToOne
	@JoinColumn(name="user_table_id",nullable=false)
	private UserTable userTable;
		
	@Column(name = "key_comment", nullable = true)
	private String comment;
	
	// a synthetic column was created by us
	@Column(name = "synth", nullable = false)
	private int synthetic;
	
	// a hidden key is one we created during mt mode - it should not be shown to a tenant.
	@Column(name = "hidden", nullable = false)
	private int hidden;
	
	// rest of these fields are for fks only
	
	// if a dangling foreign key, the target schema and table name
	@Column(name="forward_schema_name", nullable=true)
	private String referencedSchemaName;
	@Column(name="forward_table_name", nullable=true)
	private String referencedTableName;

	// if not dangling, the target table
	@ForeignKey(name="fk_key_targ_table")
	@ManyToOne
	@JoinColumn(name="referenced_table", nullable=true)
	private UserTable referencedTable;
	
	// the update/delete action
	@Column(name="fk_delete_action", nullable=true)
	private String fkDeleteAction;
	@Column(name="fk_update_action", nullable=true)
	private String fkUpdateAction;

	// if we're in emulate or loose mode, is this key persisted - i.e. did we push it down
	// if not a foreign key, always set to 1
	@Column(name="persisted",nullable=false)
	private int persisted;
	
	public Key() {
		
	}
	
	public Key(String name, IndexType kt, UserTable tab, List<KeyColumn> cols, int position) {
		this.name = name;
		this.type = kt;
		this.columns = cols;
		this.userTable = tab;
		for(KeyColumn kc : cols)
			kc.setKey(this);
		this.referencedTable = null;
		this.referencedSchemaName = null;
		this.referencedTableName = null;
		this.fkDeleteAction = null;
		this.fkUpdateAction = null;
		this.synthetic = 0;
		// most keys are persisted, except for perhaps foreign keys in emulate/ignore mode
		this.persisted = 1;
		this.hidden = 0;
		this.position = position;
	}
	
	public Key(String name, IndexType kt, UserTable tab, int position) {
		this.name = name;
		this.type = kt;
		this.userTable = tab;
		this.referencedTable = null;
		this.referencedSchemaName = null;
		this.referencedTableName = null;
		this.fkDeleteAction = null;
		this.fkUpdateAction = null;
		this.synthetic = 0;
		this.persisted = 1;
		this.hidden = 0;
		this.position = position;
	}
	
	@InfoSchemaColumn(logicalName="type", fieldName="type",
			sqlType=java.sql.Types.VARCHAR,
			views={})
	public IndexType getType() {
		return type;
	}
	
	@InfoSchemaColumn(logicalName="name", fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getName() {
		return name;
	}

	public List<KeyColumn> getColumns() {
		return columns;
	}

	public void addColumn(KeyColumn kc) {
		columns.add(kc);
		kc.setKey(this);
	}
	
	public void removeColumn(KeyColumn kc) {
		columns.remove(kc);
		kc.setKey(null);
	}
	
	@InfoSchemaColumn(logicalName="containing_table", fieldName="userTable",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public UserTable getTable() {
		return userTable;
	}

	public void setReferencedTable(UserTable ut) {
		referencedTable = ut;
		if (referencedTable != null) {
			referencedSchemaName = null;
			referencedTableName = null;
			ut.addReferring(this);
		}
	}
	
	public void setReferencedTable(String schemaName, String tableName) {
		if (tableName != null) {
			if (referencedTable != null)
				referencedTable.removeReferring(this);
			referencedTable = null;
			referencedSchemaName = schemaName;
			referencedTableName = tableName;
		}
	}
	
	@InfoSchemaColumn(logicalName="referenced_table", fieldName="referencedTable",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})	
	public UserTable getReferencedTable() {
		return referencedTable;
	}
	
	@InfoSchemaColumn(logicalName="forward_ref_table_name", fieldName="referencedTableName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})		
	public String getReferencedTableName() {
		return referencedTableName;
	}

	@InfoSchemaColumn(logicalName="forward_ref_schema_name", fieldName="referencedSchemaName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})		
	public String getReferencedSchemaName() {
		return referencedSchemaName;
	}
	public void setFKDeleteAction(String spec) {
		fkDeleteAction = spec;
	}
	
	public void setFKUpdateAction(String spec) {
		fkUpdateAction = spec;
	}
	
	@InfoSchemaColumn(logicalName="fk_delete_action", fieldName="fkDeleteAction",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getFKDeleteAction() {
		return fkDeleteAction;
	}

	@InfoSchemaColumn(logicalName="fk_update_action", fieldName="fkUpdateAction",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getFKUpdateAction() {
		return fkUpdateAction;
	}
	
	@InfoSchemaColumn(logicalName="id",fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={})	
	@Override
	public int getId() {
		return id;
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
		ArrayList<CatalogEntity> out = new ArrayList<CatalogEntity>();
		// notice we do not remove the columns themselves
		out.addAll(getColumns());
		return out;
	}
	
	public KeyValue getTemplate() {
		KeyValue colSet = new KeyValue(userTable);
		for (KeyColumn kc : columns) 
			colSet.addColumnTemplate(kc.getSourceColumn());
		return colSet;
	}
	
	public boolean containsColumn(UserColumn uc) {
		for(KeyColumn kc : columns)
			if (kc.getSourceColumn().equals(uc))
				return true;
		return false;
	}

	@InfoSchemaColumn(logicalName="constraint", fieldName="constraint",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})	
	public ConstraintType getConstraint() {
		return constraint;
	}

	public void setConstraint(ConstraintType ct) {
		constraint = ct;
	}

	@InfoSchemaColumn(logicalName="symbol", fieldName="constraintName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getSymbol() {
		return constraintName;
	}
	
	public void setSymbol(String s) {
		constraintName = s;
	}
	
	public String getPhysicalSymbol() {
		return physicalSymbol;
	}
	
	public void setPhysicalSymbol(String s) {
		physicalSymbol = s;
	}
	

	@InfoSchemaColumn(logicalName="comment", fieldName="comment",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})	
	public String getComment() {
		return comment;
	}
	
	public void setComment(String s) {
		comment = s;
	}
	public boolean isForeignKey() {
		return constraint == ConstraintType.FOREIGN;
	}
	
	public boolean isPrimaryKey() {
		return constraint == ConstraintType.PRIMARY;
	}
	
	public void setSynthetic() {
		synthetic = 1;
	}
	
	public boolean isSynthetic() {
		return synthetic != 0;
	}

	public int getPosition() {
		return position;
	}
	
	public void setPosition(int pos) {
		position = pos;
	}
	
	public void setHidden(boolean v) {
		hidden = (v ? 1 : 0);
	}
	
	public boolean isHidden() {
		return (hidden == 1);
	}
	
	@InfoSchemaColumn(logicalName="synthetic",fieldName="synthetic",
			sqlType=java.sql.Types.INTEGER,
			views={})	
	public int getSynthetic() {
		return synthetic;
	}
	
	public void setPersisted(boolean v) {
		persisted = (v ? 1 : 0);
	}
	
	public boolean isPersisted() {
		return persisted != 0;
	}
	
	public void printTempTableKey(StringBuffer buf) {
		if (constraint != null) {
			buf.append(constraint.getSQL()).append(" ");
		}
		buf.append("KEY ").append(name).append(" (");
		boolean first = true;
		for(KeyColumn kc : columns) {
			if (first) first = false;
			else buf.append(", ");
			buf.append(kc.getSourceColumn().getNameAsIdentifier());
		}
		buf.append(")");
	}
	
	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
