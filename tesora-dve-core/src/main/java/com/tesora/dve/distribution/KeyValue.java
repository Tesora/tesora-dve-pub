package com.tesora.dve.distribution;

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.datatype.XMLGregorianCalendar;

import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.compare.ComparatorCache;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.util.UnaryFunction;

@SuppressWarnings("restriction")
@XmlRootElement
public class KeyValue extends LinkedHashMap<String, ColumnDatum> implements IKeyValue {

	private static final long serialVersionUID = 1L;
	private PersistentTable userTable;
	private Integer rangeID;
	
	public KeyValue(PersistentTable ut, Integer rangeID) {
		userTable = ut;
		this.rangeID = rangeID;
	}
	
	public KeyValue(KeyValue k) {
		for (String key : k.keySet())
			put(key, new ColumnDatum(k.get(key)));
		userTable = k.userTable;
		rangeID = k.rangeID;
	}
	
	public KeyValue(PersistentTable targetTable, Integer rangeID, List<String> distColumns) throws PEException {
		userTable = targetTable;
		this.rangeID = rangeID;
		for (String colName : distColumns) {
			PersistentColumn col = userTable.getUserColumn(colName);
			if (col == null)
				throw new PEException("Column " + colName + " not found in table " + userTable.displayName());
			addColumnTemplate(col);
		}
	}

	@Override
	public DistributionModel getDistributionModel() {
		return userTable.getDistributionModel();
	}
	
	@Override
	public StorageGroup getPersistentGroup() {
		return userTable.getPersistentGroup();
	}

	@Override
	public int getPersistentGroupId() {
		return getPersistentGroup().getId();
	}
	
	public PersistentTable getUserTable() {
		return userTable;
	}
	
	@Override
	public int getUserTableId() {
		return userTable.getId();
	}
	
	@Override
	public String getQualifiedTableName() {
		return userTable.getQualifiedName();
	}
	
	public void addColumnTemplate(PersistentColumn col) {
		ColumnDatum cd = new ColumnDatum(col);
		this.put(col.getPersistentName(), cd);
	}
	
	public void populateFromResultSet(ResultSet results) throws SQLException {
		for (ColumnDatum cd : this.values()) {
			cd.setValue(results.getObject(cd.getColumn().getPersistentName()));
		}
	}

	@Override
	public boolean equals(Object o) {
		return equals((IKeyValue)this,(IKeyValue)o);
	}
	
	public static boolean equals(IKeyValue left, IKeyValue right) {
		boolean isEqual;
		try {
			isEqual = compare(left, right) == 0; 
		}
		catch (Throwable t) {
			throw new RuntimeException("Cannot compare values for equality", t);
		}
		return isEqual;
	}
	
	@Override
	public int hashCode() {
		return buildHashCode(this);
	}

	public static int buildHashCode(IKeyValue ikv) {
		int code = 0;
		for(IColumnDatum icd : ikv.getValues().values()) {
			code = code * 31 + icd.hashCode();
		}
		return code;
	}
	
	public KeyValue populateFromResultSet(ResultRow row, Map<String, Integer> colMap) {
		for (ColumnDatum cd : this.values()) {
			String colName = cd.userColumn.getAliasName();
			int colIdx = colMap.get(colName);
			ResultColumn resultCol = row.getResultColumn(colIdx+1); 
			cd.value = resultCol.getColumnValue();
		}
		return this;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T extends IColumnDatum> int compareCol(T c1, T c2) {
		int compareResult;
		
		if (c1.getComparatorClassName() != null)
			compareResult = ComparatorCache.get(c1.getComparatorClassName()).compare(c1.getValue(), c2.getValue());
		else {
			final Comparable value1 = c1.getValueForComparison();
			final Comparable value2 = c2.getValueForComparison();
			compareResult = value1.compareTo(value2);
		}
		
		return compareResult;
	}
	
	@Override
	public int compare(IKeyValue other) throws PEException {
		return compare(this,other);
	}

	public static int compare(IKeyValue left, IKeyValue right) throws PEException {
		int c = 0;
		Map<String,? extends IColumnDatum> leftValues = left.getValues();
		Map<String,? extends IColumnDatum> rightValues = right.getValues();
		if (leftValues.size() == rightValues.size()) {
			Iterator<? extends IColumnDatum> iThis = leftValues.values().iterator();
			Iterator<? extends IColumnDatum> iOther = rightValues.values().iterator();
			
			while (c == 0 && iThis.hasNext()) {
				c = compareCol(iThis.next(), iOther.next());
			}
		} else {
			throw new PEException("Cannot compare "+PEStringUtils.toString(left.getQualifiedTableName(), leftValues.keySet())
					+" to "+PEStringUtils.toString(right.getQualifiedTableName(), rightValues.keySet()));
		}
		return c;		
	}
	
	public void setUserTable(UserTable ut) {
		userTable = ut;
	}
	
	@Override
	public String toString() {
		return PEStringUtils.toString(this.userTable.getPersistentName(), this);
	}
	
	public String orderedKeyNames() {
		List<String> keyNames = Arrays.asList(keySet().toArray(new String[]{})); 
		Collections.sort(keyNames);
		String okn = keyNames.toString();
		return okn.substring(1, okn.length()-1);
	}

	@Override
	public Map<String, ? extends IColumnDatum> getValues() {
		return this;
	}

	@Override
	public DistributionModel getContainerDistributionModel() {
		if (userTable.getContainer() == null) return null;
		return userTable.getContainer().getDistributionModel();
	}
	
	private static Map<Object, UnaryFunction<Object, Object>> typePromotionMap = new HashMap<Object, UnaryFunction<Object,Object>>() {
		private static final long serialVersionUID = 1L;
		{
			put(Long.class, new UnaryFunction<Object,Object>() {
				@Override
				public Object evaluate(Object o) {
					return new Long((Long)o);
				}
			});
			put(Integer.class, new UnaryFunction<Object,Object>() {
				@Override
				public Object evaluate(Object o) {
					return new Long((Integer)o);
				}
			});
			put(Short.class, new UnaryFunction<Object,Object>() {
				@Override
				public Object evaluate(Object o) {
					return new Long(((Short)o).longValue());
				}
			});
			put(Byte.class, new UnaryFunction<Object,Object>() {
				@Override
				public Object evaluate(Object o) {
					return new Long(((Byte) o).longValue());
				}
			});
			put(Float.class, new UnaryFunction<Object,Object>() {
				@Override
				public Object evaluate(Object o) {
					return new Double((Float)o);
				}
			});
			// this is to handle the class name as returned from the xml in the generation_key_range table
			put(XMLGregorianCalendarImpl.class, new UnaryFunction<Object,Object>() {
				@Override
				public Object evaluate(Object o) {
					return ((XMLGregorianCalendar)o).toGregorianCalendar().getTime();
				}
			});
		}};

	@Override
	public int compare(RangeLimit rangeLimit) throws PEException {
		return compare(this, rangeLimit);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T extends IColumnDatum> int compareCol(T c1, Object c2) {
		int compareResult;
		if (c1.getComparatorClassName() != null)
			compareResult = ComparatorCache.get(c1.getComparatorClassName()).compare(c1.getValue(), c2);
		else {
			final Comparable value1 = c1.getValueForComparison();
			if (value1.getClass() != c2.getClass() && typePromotionMap.containsKey(c2.getClass()))
				compareResult = value1.compareTo(typePromotionMap.get(c2.getClass()).evaluate(c2));
			else
				compareResult = value1.compareTo(c2);
		}
		
		return compareResult;
	}
	
	public static int compare(IKeyValue keyValue, RangeLimit rangeLimit) throws PEException {
		int result = 0;
		Map<String,? extends IColumnDatum> keyValueMap = keyValue.getValues();
		if (keyValueMap.size() == rangeLimit.size()) {
			Iterator<? extends IColumnDatum> iKeyValue = keyValueMap.values().iterator();
			Iterator<Object> iRangeLimit = rangeLimit.iterator();
			
			while (result == 0 && iKeyValue.hasNext()) {
				result = compareCol(iKeyValue.next(), iRangeLimit.next());
			}
		} else {
			throw new PEException("Cannot compare "+PEStringUtils.toString(keyValue.getQualifiedTableName(), keyValueMap.keySet())
					+" to "+ rangeLimit.toString());
		}
		return result;		
	}

	@Override
	public Integer getRangeId() {
		return rangeID;
	}

	@Override
	public IKeyValue rebind(ConnectionValues cv) {
		return this;
	}

}
