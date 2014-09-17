package com.tesora.dve.sql.schema.types;

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

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.UnqualifiedName;

// only one of these - represents the type when we don't know what the type is
public class UnknownType implements Type {

	public static final UnknownType INSTANCE = new UnknownType();
	
	private UnknownType() {
	}

	@Override
	public NativeType getBaseType() {
		return null;
	}

	@Override
	public MysqlNativeType getMysqlType() {
		return null;
	}

	@Override
	public boolean isUnsigned() {
		return false;
	}

	@Override
	public boolean isZeroFill() {
		return false;
	}

	@Override
	public boolean isBinaryText() {
		return false;
	}

	@Override
	public int getSize() {
		return 0;
	}

	@Override
	public boolean hasSize() {
		return false;
	}

	@Override
	public boolean hasPrecisionAndScale() {
		return false;
	}

	@Override
	public int getPrecision() {
		return 0;
	}

	@Override
	public int getScale() {
		return 0;
	}

	@Override
	public UnqualifiedName getCharset() {
		return null;
	}

	@Override
	public UnqualifiedName getCollation() {
		return null;
	}

	@Override
	public Integer getIndexSize() {
		return null;
	}

	@Override
	public TextType asTextType() {
		return null;
	}

	@Override
	public String getComparison() {
		return null;
	}

	@Override
	public boolean declUsesSizing() {
		return false;
	}

	@Override
	public boolean isSerialPlaceholder() {
		return false;
	}

	@Override
	public void addColumnTypeModifiers(UserColumn uc) {
	}

	@Override
	public void addColumnTypeModifiers(CatalogColumnEntity cce)
			throws PEException {
	}

	@Override
	public String getTypeName() {
		return null;
	}

	@Override
	public void persistTypeName(UserColumn uc) {
	}

	
	@Override
	public Integer getDataType() {
		return null;
	}

	@Override
	public boolean comparableForDistribution(Type t) {
		return false;
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public boolean isUnknown() {
		return true;
	}

	@Override
	public boolean mustParameterize() {
		return false;
	}

	@Override
	public boolean isBinaryType() {
		return false;
	}

	@Override
	public boolean supportsDefaultValue() {
		return false;
	}

	@Override
	public boolean isStringType() {
		return false;
	}

	@Override
	public boolean isFloatType() {
		return false;
	}

	@Override
	public boolean isNumericType() {
		return false;
	}

	@Override
	public boolean isDecimalType() {
		return false;
	}

	@Override
	public boolean isIntegralType() {
		return false;
	}

	@Override
	public boolean isBitType() {
		return false;
	}

	@Override
	public boolean isTimestampType() {
		return false;
	}

	@Override
	public boolean asKeyRequiresPrefix() {
		return false;
	}

	@Override
	public LiteralExpression getZeroValueLiteral() {
		return null;
	}

	@Override
	public Type normalize() {
		return this;
	}

	@Override
	public boolean isAcceptableColumnTypeForRangeType(Type columnType) {
		return false;
	}

	@Override
	public boolean isAcceptableRangeType() {
		return false;
	}

	@Override
	public TextType toTextType() {
		return null;
	}

}
