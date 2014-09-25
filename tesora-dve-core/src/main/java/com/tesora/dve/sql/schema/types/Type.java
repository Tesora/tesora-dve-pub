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

public interface Type {

	public abstract NativeType getBaseType();

	public abstract MysqlNativeType getMysqlType();

	public abstract boolean isUnsigned();

	public abstract boolean isZeroFill();

	public abstract boolean isBinaryText();

	public abstract int getSize();

	public abstract boolean hasSize();

	public abstract boolean hasPrecisionAndScale();

	public abstract int getPrecision();

	public abstract int getScale();

	public abstract UnqualifiedName getCharset();

	public abstract UnqualifiedName getCollation();

	public abstract Integer getIndexSize();

	public abstract TextType asTextType();

	public abstract String getComparison();

	public abstract boolean declUsesSizing();

	public abstract boolean isSerialPlaceholder();

	public abstract void addColumnTypeModifiers(UserColumn uc);

	public abstract void addColumnTypeModifiers(CatalogColumnEntity cce)
			throws PEException;

	public abstract String getTypeName();

	public abstract void persistTypeName(UserColumn uc);
	
	public abstract Integer getDataType();

	public abstract boolean comparableForDistribution(Type t);

	// simplified name
	public abstract String getName();

	public abstract boolean isUnknown();
	
	public abstract boolean mustParameterize();

	public abstract boolean isBinaryType();

	public abstract boolean supportsDefaultValue();

	public abstract boolean isStringType();

	public abstract boolean isFloatType();

	public abstract boolean isNumericType();

	public abstract boolean isDecimalType();

	public abstract boolean isIntegralType();

	public abstract boolean isBitType();

	public abstract boolean isTimestampType();

	public abstract boolean asKeyRequiresPrefix();

	public abstract LiteralExpression getZeroValueLiteral();

	public abstract Type normalize();

	// determining whether types are range dist compatible.  the default is they are only compatible if they
	// are exactly the same (upto flags like unsigned, zerofill).  we relax these requirements for some types.
	public abstract boolean isAcceptableColumnTypeForRangeType(
			Type columnType);

	// generally types are acceptable
	public abstract boolean isAcceptableRangeType();

	public TextType toTextType();
	
	public int getColumnAttributesFlags();

}