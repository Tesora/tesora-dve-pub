package com.tesora.dve.resultset;

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

import java.io.Serializable;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.mysql.common.ColumnAttributes;

public class ColumnSet implements Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	private static final Map<Integer, ColumnMetadata> TYPE_MAP = 
	Collections.unmodifiableMap(new HashMap<Integer, ColumnMetadata>(){{
		put(Types.VARCHAR, new ColumnMetadata("Value", ColumnAttributes.SIZED_TYPE, 255, "varchar", Types.VARCHAR));
		put(Types.INTEGER, new ColumnMetadata("Value", ColumnAttributes.SIZED_TYPE, 4, "int", Types.INTEGER));
		put(Types.BOOLEAN, new ColumnMetadata("Value", 0, 1, "boolean", Types.BOOLEAN));
	}});

	protected List<ColumnMetadata> columns;

	public ColumnSet() {
		columns = new ArrayList<ColumnMetadata>();
	}
	
	public ColumnSet(ColumnSet other) {
		columns = new ArrayList<ColumnMetadata>(other.columns);
	}

	public ColumnSet(int initialCapacity) {
		columns = new ArrayList<ColumnMetadata>(initialCapacity);
	}

	public int size() {
		return columns.size();
	}

	public ColumnSet addColumn(ColumnMetadata cm) {
		this.columns.add(cm);
		return this;
	}

	public ColumnSet setColumn(int pos, ColumnMetadata cm) {
		this.columns.set(pos, cm);
		return this;
	}

	public ColumnSet addColumn(String name, int length, String nativeType, int SQLtype, int precision, int scale) {
		ColumnMetadata cmc = createBasicColumn(name, length, nativeType, SQLtype);
		cmc.setPrecision(precision);
		cmc.setScale(scale);
		return addColumn(cmc);
	}

	public ColumnSet addColumn(String name, int length, String nativeType, int SQLtype) {
		return addColumn(createBasicColumn(name,length,nativeType,SQLtype));
	}
	
	public ColumnSet addNullableColumn(String name, int length, String nativeType, int SQLtype) {
		ColumnMetadata cmc = createBasicColumn(name, length, nativeType, SQLtype);
		cmc.setNullable(true);
		return addColumn(cmc);
	}

	private ColumnMetadata createBasicColumn(String name, int length, String nativeType, int SQLtype) {
		ColumnMetadata cmc = new ColumnMetadata();
		cmc.setName(name);
		cmc.setSize(length);
		cmc.setTypeName(nativeType);
		cmc.setDataType(SQLtype);
		return cmc;
	}
	
	public ColumnMetadata getColumn(int index) {
		return columns.get(index - 1);
	}

	public final List<ColumnMetadata> getColumnList() {
		return columns;
	}
	
	public void setColumnList(List<ColumnMetadata> columns) {
		this.columns = columns;
	}

	public long calculateRowSize() {
		long rowSize = 0;
		for (ColumnMetadata cm : columns) {
			// exclude BLOB/CLOB from calculation
			if (cm.getDataType() != Types.BLOB
					&& cm.getDataType() != Types.LONGVARBINARY
					&& cm.getDataType() != Types.CLOB
					&& cm.getDataType() != Types.LONGVARCHAR)
				rowSize += cm.getSize();
		}

		return rowSize;
	}

	public Map<String, Integer> getColumnMap(boolean useAliases) {
		Map<String, Integer> colMap = new HashMap<String, Integer>();
		for (int i = 0; i < columns.size(); i++) {
			ColumnMetadata cm = columns.get(i);
			// if the caller ask to use aliases, still only use if alias set on the ColumnMetadata
			colMap.put((useAliases ? (cm.usingAlias() ?  cm.getAliasName() : cm.getName()) : cm.getName()), i);
		}
		return colMap;
	}

	public static ColumnSet singleColumn(String alias, int type) {
		ColumnMetadata template = TYPE_MAP.get(type);
		ColumnSet cs = new ColumnSet();
		cs.addColumn(alias, template.getSize(), template.getTypeName(), type);
		return cs;
	}
}
