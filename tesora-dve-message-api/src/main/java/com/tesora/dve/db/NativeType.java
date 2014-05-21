package com.tesora.dve.db;

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
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.exceptions.PEException;

public abstract class NativeType implements Serializable {
	private static final long serialVersionUID = 1L;

	private String typeName;
	private int nativeTypeId;
	private int dataType;
	private boolean jdbcType;
	private long precision = 0;
	private String literalPrefix = "";
	private String literalSuffix = "";
	private String createParams = ""; // TODO haven't really figured out how to use this yet
	private boolean supportsPrecision = false;
	private boolean supportsScale = false;
	private short nullability = DatabaseMetaData.typeNullableUnknown;
	private boolean caseSensitive = false;
	private int searchable = DatabaseMetaData.typeSearchable;
	private boolean unsignedAttribute = false;
	private boolean autoIncrement = false;
	private int minimumScale = 0;
	private int maximumScale = 0;
	private int numPrecRadix = 10;
	protected List<NativeTypeAlias> aliases;
	private boolean comparable = true; // recognizing that most types are comparable
	private boolean usedInCreate = true; // can this type be used in CREATE stmts 
	private long maxPrecision = 0; // we are using precision in some cases as "default precision". We need to store a max (PE-1232)

	@SuppressWarnings("unused")
	private NativeType() {
	}

	public NativeType(String typeName, int nativeTypeId, int dataType, boolean jdbcType) {
		this(typeName, nativeTypeId, dataType, jdbcType, null);
	}

	public NativeType(String typeName, int nativeTypeId, int dataType, boolean jdbcType, NativeTypeAlias[] aliases) {
		setTypeName(typeName);
		setNativeTypeId(nativeTypeId);
		setDataType(dataType);
		this.jdbcType = jdbcType;
		setAliases(aliases);
	}
	
	/**
	 * Make sure the type name is always the same.  Sets the name to lower case with spaces instead of '_'
	 * 
	 * Does the 'opposite' of fixNameForEnum.
	 * 
	 * @param name
	 * @return
	 */
	public static String fixName(String name) {
		return fixName(name, false);
	}

	/**
	 * Make sure the type name is always the same.  Sets the name to lower case with spaces instead of '_'
	 * 
	 * Does the 'opposite' of fixNameForEnum.
	 * 
	 * @param name
	 * @return
	 */
	public static String fixName(String name, boolean stripEnum) {
		final String[] matches = new String[] { "enum", "set" };
		final String[] replacements = new String[] { "enum", "set" };
		for(int i = 0; i < matches.length; i++) {
			if (StringUtils.startsWith(StringUtils.lowerCase(name, Locale.ENGLISH), matches[i])) {
				if (stripEnum) {
					return replacements[i];
				}
				return matches[i] + name.substring(matches[i].length());
			}
			
		}

		return name == null ? null : StringUtils.lowerCase(name.replaceAll("_", " "),Locale.ENGLISH);
	}

	/**
	 * This method is to make sure that when mapping a string to a type (through, say, enum.valueOf), the format is always
	 * the same. Inherited classes should use this to set up their own enum's of valid types.
	 * 
	 * Does the 'opposite' of fixName.
	 * 
	 * @param name
	 * @return
	 */
	public static String fixNameForType(String name) {
		if (StringUtils.startsWith(StringUtils.lowerCase(name, Locale.ENGLISH), "enum")) {
			return "ENUM";
		}

		return name == null ? null : StringUtils.replace(name.toUpperCase(Locale.ENGLISH)," ", "_");
	}

	
	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = fixName(typeName);
	}

	@SuppressWarnings("hiding")
	public NativeType withTypeName(String typeName) {
		setTypeName(typeName);
		return this;
	}

	public boolean requiresQuotes() {
		return (!this.literalPrefix.isEmpty() && !this.literalSuffix.isEmpty());
	}

	public void setQuoteCharacter(String quote) {
		this.literalPrefix = quote;
		this.literalSuffix = quote;
	}

	public NativeType withQuoteCharacter(String quote) {
		setQuoteCharacter(quote);
		return this;
	}

	public int getDataType() {
		return this.dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public int getNativeTypeId() {
		return nativeTypeId;
	}

	public void setNativeTypeId(int nativeTypeId) {
		this.nativeTypeId = nativeTypeId;
	}

	public boolean isJdbcType() {
		return jdbcType;
	}

	@SuppressWarnings("hiding")
	public NativeType withDataType(int dataType) {
		setDataType(dataType);
		return this;
	}

	public long getPrecision() {
		return precision;
	}

	public void setPrecision(long precision) {
		this.precision = precision;
	}

	@SuppressWarnings("hiding")
	public NativeType withPrecision(long precision) {
		setPrecision(precision);
		setMaxPrecision(precision);
		return this;
	}

	public long getMaxPrecision() {
		return maxPrecision;
	}

	public void setMaxPrecision(long maxPrecision) {
		this.maxPrecision = maxPrecision;
	}

	@SuppressWarnings("hiding")
	public NativeType withMaxPrecision(long maxPrecision) {
		setMaxPrecision(maxPrecision);
		return this;
	}

	public String getLiteralPrefix() {
		return literalPrefix;
	}

	public void setLiteralPrefix(String literalPrefix) {
		this.literalPrefix = literalPrefix;
	}

	@SuppressWarnings("hiding")
	public NativeType withLiteralPrefix(String literalPrefix) {
		setLiteralPrefix(literalPrefix);
		return this;
	}

	public String getLiteralSuffix() {
		return literalSuffix;
	}

	public void setLiteralSuffix(String literalSuffix) {
		this.literalSuffix = literalSuffix;
	}

	@SuppressWarnings("hiding")
	public NativeType withLiteralSuffix(String literalSuffix) {
		setLiteralSuffix(literalSuffix);
		return this;
	}

	public String getCreateParams() {
		return createParams;
	}

	public void setCreateParams(String createParams) {
		this.createParams = createParams;
	}

	@SuppressWarnings("hiding")
	public NativeType withCreateParams(String createParams) {
		setCreateParams(createParams);
		return this;
	}

	public short getNullability() {
		return nullability;
	}

	public boolean isNullable() {
		return (nullability == DatabaseMetaData.typeNullable ? true : false);
	}

	public NativeType isNullable(boolean nullable) {
		setNullability((short) (nullable ? DatabaseMetaData.typeNullable
				: DatabaseMetaData.typeNoNulls));
		return this;
	}

	public void setNullability(short nullability) {
		this.nullability = nullability;
	}

	@SuppressWarnings("hiding")
	public NativeType withNullability(short nullability) {
		setNullability(nullability);
		return this;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public void setCaseSensitive(boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}

	@SuppressWarnings("hiding")
	public NativeType withCaseSensitive(boolean caseSensitive) {
		setCaseSensitive(caseSensitive);
		return this;
	}

	public int getSearchable() {
		return searchable;
	}

	public void setSearchable(int searchable) {
		this.searchable = searchable;
	}

	@SuppressWarnings("hiding")
	public NativeType withSearchable(int searchable) {
		setSearchable(searchable);
		return this;
	}

	public boolean isUnsignedAttribute() {
		return unsignedAttribute;
	}

	public void setUnsignedAttribute(boolean unsignedAttribute) {
		this.unsignedAttribute = unsignedAttribute;
	}

	@SuppressWarnings("hiding")
	public NativeType withUnsignedAttribute(boolean unsignedAttribute) {
		setUnsignedAttribute(unsignedAttribute);
		return this;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}

	@SuppressWarnings("hiding")
	public NativeType withAutoIncrement(boolean autoIncrement) {
		setAutoIncrement(autoIncrement);
		return this;
	}

	public int getMinimumScale() {
		return minimumScale;
	}

	public void setMinimumScale(int minimumScale) {
		this.minimumScale = minimumScale;
	}

	@SuppressWarnings("hiding")
	public NativeType withMinimumScale(int minimumScale) {
		setMinimumScale(minimumScale);
		return this;
	}

	public int getMaximumScale() {
		return maximumScale;
	}

	public void setMaximumScale(int maximumScale) {
		this.maximumScale = maximumScale;
	}

	@SuppressWarnings("hiding")
	public NativeType withMaximumScale(int maximumScale) {
		setMaximumScale(maximumScale);
		return this;
	}

	public int getNumPrecRadix() {
		return numPrecRadix;
	}

	public void setNumPrecRadix(int numPrecRadix) {
		this.numPrecRadix = numPrecRadix;
	}

	@SuppressWarnings("hiding")
	public NativeType withNumPrecRadix(int numPrecRadix) {
		setNumPrecRadix(numPrecRadix);
		return this;
	}

	public List<NativeTypeAlias> getAliases() {
		return aliases;
	}

	public void setAliases(List<NativeTypeAlias> aliases) {
		this.aliases = aliases;
	}

	public void setAliases(NativeTypeAlias[] aliases) {
		if ( aliases != null ) {
			this.aliases = new ArrayList<NativeTypeAlias>(Arrays.asList(aliases));
		} else
			this.aliases = new ArrayList<NativeTypeAlias>();
	}

	public Set<String> getNames() {
		Set<String> names = new HashSet<String>();
		names.add(getTypeName());
		for (NativeTypeAlias alias : aliases) {
			names.add(fixName(alias.getAliasName()));
		}
		
		return names;
	}
	
	public boolean getSupportsPrecision() {
		return supportsPrecision;
	}

	public void setSupportsPrecision(boolean supportsPrecision) {
		this.supportsPrecision = supportsPrecision;
	}

	@SuppressWarnings("hiding")
	public NativeType withSupportsPrecision(boolean supportsPrecision) {
		setSupportsPrecision(supportsPrecision);
		return this;
	}

	public boolean getSupportsScale() {
		return supportsScale;
	}

	public void setSupportsScale(boolean supportsScale) {
		this.supportsScale = supportsScale;
	}

	@SuppressWarnings("hiding")
	public NativeType withSupportsScale(boolean supportsScale) {
		setSupportsScale(supportsScale);
		return this;
	}

	public boolean isComparable() {
		return comparable;
	}

	public void setComparable(boolean comparable) {
		this.comparable = comparable;
	}

	@SuppressWarnings("hiding")
	public NativeType withComparable(boolean comparable) {
		setComparable(comparable);
		return this;
	}

	public boolean isUsedInCreate() {
		return usedInCreate;
	}

	public void setUsedInCreate(boolean usedInCreate) {
		this.usedInCreate = usedInCreate;
	}

	@SuppressWarnings("hiding")
	public NativeType withUsedInCreate(boolean usedInCreate) {
		setUsedInCreate(usedInCreate);
		return this;
	}

	public boolean isBinaryType() {
		return dataType == Types.LONGVARBINARY || dataType == Types.BLOB ||
				dataType == Types.BINARY || dataType == Types.VARBINARY;
	}

	public boolean isStringType() {
		return dataType == Types.VARCHAR || dataType == Types.LONGVARCHAR ||
				dataType == Types.CHAR || dataType == Types.CLOB ||
				dataType == Types.LONGNVARCHAR;
	}

	public boolean supportsDefaultValue() {
		return true;
	}
	
	public boolean isFloatType() {
		return dataType == Types.FLOAT || dataType == Types.REAL || dataType == Types.DOUBLE;
	}

	public boolean isNumericType() {
		return dataType == Types.BIGINT || dataType == Types.DECIMAL || dataType == Types.DOUBLE
				|| dataType == Types.FLOAT || dataType == Types.INTEGER || dataType == Types.NUMERIC
				|| dataType == Types.REAL || dataType == Types.TINYINT || dataType == Types.SMALLINT;
	}

	public boolean isDecimalType() {
		return dataType == Types.DECIMAL || dataType == Types.NUMERIC;
	}

	public boolean isTimestampType() {
		return dataType == Types.TIMESTAMP;
	}

	public boolean isIntegralType() {
		return dataType == Types.BIGINT || dataType == Types.INTEGER || dataType == Types.TINYINT || dataType == Types.SMALLINT;
	}
	
	public boolean asKeyRequiresPrefix() {
		return false;
	}

	// not always correct, but presumably the subtypes will do the right thing
	public Object getZeroValue() throws PEException {
		return "";
	}
		
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (autoIncrement ? 1231 : 1237);
		result = prime * result + (caseSensitive ? 1231 : 1237);
		result = prime * result + (comparable ? 1231 : 1237);
		result = prime * result + ((createParams == null) ? 0 : createParams.hashCode());
		result = prime * result + dataType;
		result = prime * result + (jdbcType ? 1231 : 1237);
		result = prime * result + ((literalPrefix == null) ? 0 : literalPrefix.hashCode());
		result = prime * result + ((literalSuffix == null) ? 0 : literalSuffix.hashCode());
		result = prime * result + maximumScale;
		result = prime * result + minimumScale;
		result = prime * result + nullability;
		result = prime * result + numPrecRadix;
		result = prime * result + (int) (precision ^ (precision >>> 32));
		result = prime * result + searchable;
		result = prime * result + (supportsPrecision ? 1231 : 1237);
		result = prime * result + (supportsScale ? 1231 : 1237);
		result = prime * result + ((typeName == null) ? 0 : typeName.hashCode());
		result = prime * result + (unsignedAttribute ? 1231 : 1237);
		result = prime * result + (usedInCreate ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NativeType other = (NativeType) obj;
		if (autoIncrement != other.autoIncrement)
			return false;
		if (caseSensitive != other.caseSensitive)
			return false;
		if (comparable != other.comparable)
			return false;
		if (createParams == null) {
			if (other.createParams != null)
				return false;
		} else if (!createParams.equals(other.createParams))
			return false;
		if (dataType != other.dataType)
			return false;
		if (jdbcType != other.jdbcType)
			return false;
		if (literalPrefix == null) {
			if (other.literalPrefix != null)
				return false;
		} else if (!literalPrefix.equals(other.literalPrefix))
			return false;
		if (literalSuffix == null) {
			if (other.literalSuffix != null)
				return false;
		} else if (!literalSuffix.equals(other.literalSuffix))
			return false;
		if (maximumScale != other.maximumScale)
			return false;
		if (minimumScale != other.minimumScale)
			return false;
		if (nullability != other.nullability)
			return false;
		if (numPrecRadix != other.numPrecRadix)
			return false;
		if (precision != other.precision)
			return false;
		if (searchable != other.searchable)
			return false;
		if (supportsPrecision != other.supportsPrecision)
			return false;
		if (supportsScale != other.supportsScale)
			return false;
		if (typeName == null) {
			if (other.typeName != null)
				return false;
		} else if (!typeName.equals(other.typeName))
			return false;
		if (unsignedAttribute != other.unsignedAttribute)
			return false;
		if (usedInCreate != other.usedInCreate)
			return false;
		return true;
	}

}
