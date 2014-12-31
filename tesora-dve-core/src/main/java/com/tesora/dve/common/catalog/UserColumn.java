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


import java.sql.Types;
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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

import org.hibernate.annotations.ForeignKey;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.db.mysql.common.ColumnAttributes;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;


/**
 * Entity implementation class for Entity: UserColumn
 *
 */
@Entity
@Table(name="user_column")
public class UserColumn implements CatalogEntity, PersistentColumn {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name="user_column_id")
	private int id;
	
	@ForeignKey(name="fk_column_table")
	@ManyToOne
	@JoinColumn(name="user_table_id")
	private UserTable userTable;
	
	@Column(name="name",nullable=false)
	private String name;
	@Column(name="data_type",nullable=false)
	private int dataType;
	// native_type_name is the simple type name, i.e. decimal, set, enum, blob.
	@Column(name="native_type_name",nullable=false)
	private String nativeTypeName;
	// enum/set allowed values
	@Lob @Column(name="es_universe")
	private String es_universe;
	@Column(name="size", nullable=false)
	private int size = 0;
	@Column(name="hash_position")
	private int hashPosition = 0;
	@Column(name="prec", nullable=false)
	private int precision = 0;
	@Column(name="scale", nullable=false)
	private int scale = 0;
	@Column(name="flags", nullable=false)
	private int flags = 0;
	@Lob @Column(name="default_value")
	private String defaultValue = "";
	@Column(name = "order_in_table",nullable=false)
	private Integer orderInTable;
	@Column(name="cdv")
	private int cdv_position = 0;
	@Column (name="comment",nullable=true)
	private String comment;
	@Column (name="charset",nullable=true)
	private String charset;
	@Column (name="collation",nullable=true)
	private String collation;

	private transient ColumnSet showColumnSet = null;
	private transient boolean nativeTypeSet = false;
	private transient byte nativeType;

	public UserColumn() {
	}   

	// Copy Constructor
	public UserColumn( UserColumn uc ) {
		this.copyFrom(uc);
	}   

	// Create a UserColumn entity from a ColumnMetadata object
	public UserColumn ( ColumnMetadata cm ) throws PEException {
        Singletons.require(DBNative.class).convertColumnMetadataToUserColumn(cm, this);
	}
	
	public ColumnMetadata getColumnMetadata() {
		ColumnMetadata cm = new ColumnMetadata(name, flags, dataType, nativeTypeName);
		cm.setDefaultValue(defaultValue);
		cm.setHashPosition(hashPosition);
		cm.setESUniverse(es_universe);
		cm.setOrderInTable(orderInTable);
		cm.setPrecision(precision);
		cm.setScale(scale);
		cm.setSize(size);
		if (getUserTable() != null)
			cm.setTableName(getUserTable().getName());

		return cm;
	}
	
	public UserColumn(UserTable ut, String name, int dataType, String nativeTypeName, int size) {
		this.name = name;
		this.dataType = dataType;
		this.nativeTypeName = nativeTypeName;
		this.size = size;
		this.userTable = ut;
		ut.addUserColumn(this);
	}

	public UserColumn(UserTable ut, String name, int dataType, String nativeTypeName) {
		this.name = name;
		this.dataType = dataType;
		this.nativeTypeName = nativeTypeName;
		this.userTable = ut;
		ut.addUserColumn(this);
	}

	public UserColumn(String name, int dataType, String nativeTypeName) {
		this.name = name;
		this.dataType = dataType;
		this.nativeTypeName = nativeTypeName;
	}

    public void copyFrom(UserColumn uc) {
        name = uc.name;
        dataType = uc.dataType;
        nativeTypeName = uc.nativeTypeName;
        es_universe = uc.es_universe;
        size = uc.size;
        userTable = uc.userTable;
        flags = uc.flags;
        defaultValue = uc.defaultValue;
        precision = uc.precision;
        scale = uc.scale;
        hashPosition = uc.hashPosition;
        comment = uc.comment;
    }

	@Override
	public int getId() {
		return id;
	}

	public UserTable getUserTable() {
		return userTable;
	}
	public void setUserTable( UserTable userTable ) {
		this.userTable = userTable;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}   

	public String getNameAsIdentifier() {
        return Singletons.require(DBNative.class).getNameForQuery(this);
	}
	
	public String getQueryName() {
		return getName();
	}

	public String getAliasName() {
		return getName();
	}
	
	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}   
	
	public String getFullTypeName() {
		// so typeName is just the name
		// we need to figure out if we also need to add the size
		StringBuilder buf = new StringBuilder();
		buf.append(nativeTypeName);
		if (ColumnAttributes.isSet(flags, ColumnAttributes.SIZED_TYPE)) {
			buf.append("(").append(size).append(")");
		} else if (ColumnAttributes.isSet(flags, ColumnAttributes.PS_TYPE)) {
			buf.append("(").append(precision).append(",").append(scale).append(")");
		}
		if (es_universe != null)
			buf.append("(").append(es_universe).append(")");
		if (ColumnAttributes.isSet(flags, ColumnAttributes.UNSIGNED))
			buf.append(" unsigned");
		if (ColumnAttributes.isSet(flags, ColumnAttributes.ZEROFILL))
			buf.append(" zerofill");
		return buf.toString();
	}
	
	public String getTypeName() {
		return nativeTypeName;
	}
	
	public void setTypeName(String nativeTypeName) {
		this.nativeTypeName = nativeTypeName;
	}

	public String getESUniverse() {
		return es_universe;
	}
	
	public void setESUniverse(String v) {
		es_universe = v;
	}
	
	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
	
	public int getHashPosition() {
		return hashPosition;
	}

	public void setHashPosition(int hashPosition) {
		this.hashPosition = hashPosition;
	}   

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}
	
	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}   
	public Boolean isNullable() {
		return getNullable();
	}

	public Boolean getNullable() {
		return !ColumnAttributes.isSet(flags, ColumnAttributes.NOT_NULLABLE);
	}

	public void setNullable(Boolean nullable) {
		if (nullable == null || Boolean.FALSE.equals(nullable))
			flags = ColumnAttributes.set(flags, ColumnAttributes.NOT_NULLABLE);
		else
			flags = ColumnAttributes.clear(flags, ColumnAttributes.NOT_NULLABLE);
	}   
	
	public void setFlags(int flags) {
		this.flags = flags;
	}
	
	public int getFlags() {
		return this.flags;
	}
	
	public Boolean hasDefault() {
		return getHasDefault();
	}

	public Boolean getHasDefault() {
		return ColumnAttributes.isSet(flags, ColumnAttributes.HAS_DEFAULT_VALUE);
	}

	public void setHasDefault(Boolean hasDefault) {
		flags = ColumnAttributes.set(flags, ColumnAttributes.HAS_DEFAULT_VALUE);
	}   

	public Boolean isAutoGenerated() {
		return getAutoGenerated();
	}

	public Boolean getAutoGenerated() {
		return ColumnAttributes.isSet(flags, ColumnAttributes.AUTO_INCREMENT);
	}
	
	public void setAutoGenerated(Boolean autoGenerated) {
		if (autoGenerated == null || Boolean.FALSE.equals(autoGenerated))
			flags = ColumnAttributes.clear(flags, ColumnAttributes.AUTO_INCREMENT);
		else
			flags = ColumnAttributes.set(flags, ColumnAttributes.AUTO_INCREMENT);
	}

	public String getDefaultValue() {
		if (ColumnAttributes.isSet(flags, ColumnAttributes.HAS_DEFAULT_VALUE))
			return this.defaultValue;
		return null;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}   
	
	@Override
	public boolean equals(Object o) {
		if ( this == o )
			return true;
		boolean isEqual = false;
		if (o instanceof UserColumn) {
			UserColumn oCol = (UserColumn)o;
			isEqual 
				= StringUtils.equals(userTable.getName(),oCol.userTable.getName())
				&& StringUtils.equals(userTable.getDatabase().getName(),oCol.userTable.getDatabase().getName())
				&& StringUtils.equals(name, oCol.name)
				&& dataType == oCol.dataType
				&& nativeTypeName.equals(oCol.nativeTypeName)
				&& size == oCol.size
				&& hashPosition == oCol.hashPosition
				&& precision == oCol.precision
				&& scale == oCol.scale
				&& flags == oCol.flags
				&& StringUtils.equals(defaultValue, oCol.defaultValue)
				&& cdv_position == oCol.cdv_position;
		}
		
		return isEqual;
	}
    
	public Integer getOrderInTable() {
		return orderInTable;
	}

	public void setOrderInTable(Integer orderInTable) {
		this.orderInTable = orderInTable;
	}

	// used for computing comparable dvs
	public boolean comparableType(UserColumn other) {
		return (dataType == other.dataType &&
				nativeTypeName.equals(other.nativeTypeName) &&
				precision == other.precision &&
				scale == other.scale &&
				size == other.size);
	}
	
	@Override
	public String toString()
	{
		return "Column ID: " + getId() + " Name: " + getName() + 
			" Type:" + getDataType() +
			" Flags:" + getFlags() +
			" Native Type: " + getFullTypeName() +
			" In Table: " + (userTable == null ? "Unknown" : getUserTable());
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Field", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Type", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Null",3,"varchar", Types.VARCHAR);
			showColumnSet.addColumn("Key",3,"varchar", Types.VARCHAR);
			showColumnSet.addColumn("Default",255,"varchar", Types.VARCHAR);
			showColumnSet.addColumn("Extra",255,"varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) throws PEException {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
        String type = Singletons.require(DBNative.class).getDataTypeForQuery(this);
		rr.addResultColumn(type, false);
		rr.addResultColumn(isNullable() ? "YES" : "NO");
		rr.addResultColumn(this.isPrimaryKeyPart()? "PRI" : "" );
		rr.addResultColumn(hasDefault() ? this.defaultValue : "NULL");
		rr.addResultColumn(this.isAutoGenerated() ? "auto_increment" : "" );
				
		return rr;
	}

	public String getShowExtra() {
		return (this.isAutoGenerated() ? "auto_increment" : "");
	}
	
	public String getShowType() throws PEException {
        return Singletons.require(DBNative.class).getDataTypeForQuery(this);
	}

	public String getShowKey() {
		return (isPrimaryKeyPart() ? "PRI" : "");
	}

	
	public boolean isPrimaryKeyPart() {
		Key pk = userTable.getPrimaryKey();
		if (pk == null) return false;
		return pk.containsColumn(this);
	}
	
	@Override
	public void removeFromParent() throws Throwable {
		userTable.removeUserColumn(this);
	}
	
	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// TODO Return a valid list of dependents
		return Collections.emptyList();
	}

	public Boolean hasOnUpdate() {
		return ColumnAttributes.isSet(flags, ColumnAttributes.ONUPDATE);
	}
	
	public void setOnUpdate(Boolean onUpdateValue) {
		flags = ColumnAttributes.set(flags, ColumnAttributes.ONUPDATE, onUpdateValue);
	}
	
	public boolean isUnsigned() {
		return ColumnAttributes.isSet(flags, ColumnAttributes.UNSIGNED);
	}
	
	public void setUnsigned(boolean v) {
		flags = ColumnAttributes.set(flags, ColumnAttributes.UNSIGNED, v);
	}
	
	public boolean isZerofill() {
		return ColumnAttributes.isSet(flags, ColumnAttributes.ZEROFILL);
	}
	
	public void setZerofill(boolean v) {
		flags = ColumnAttributes.set(flags, ColumnAttributes.ZEROFILL, v);
	}
	
	public boolean isBinaryText() {
		return ColumnAttributes.isSet(flags, ColumnAttributes.BINARY);
	}
	
	public void setBinaryText(boolean v) {
		flags = ColumnAttributes.set(flags, ColumnAttributes.BINARY, v);
	}
	
	public int getCDV_Position() {
		return cdv_position;
	}

	public void setCDV_Position(int offset) {
		cdv_position = offset;
	}

	public String getComment() {
		return comment;
	}
	
	public void setComment(String s) {
		comment = s;
	}
	
	public void setCollation(String s) {
		collation = s;
	}
	
	public String getCollation() {
		return collation;
	}
	
	public void setCharset(String s) {
		charset = s;
	}

	public String getCharset() {
		return charset;
	}
	
	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

	public byte getUserTypeByte() {
		if (!nativeTypeSet) {
			nativeType = MysqlType.toMysqlType(nativeTypeName).getMysqlFieldType().getByteValue();
		}
		return nativeType;
	}

	@Override
	public String getPersistentName() {
		return getName();
	}
}
