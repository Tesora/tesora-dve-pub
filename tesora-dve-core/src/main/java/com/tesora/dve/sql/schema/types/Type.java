// OS_STATUS: public
package com.tesora.dve.sql.schema.types;

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

	public abstract Integer getDataType();

	public abstract boolean comparableForDistribution(Type t);

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

}