// OS_STATUS: public
package com.tesora.dve.sql.schema.types;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.UnqualifiedName;


public class TempColumnType implements Type {

	// this is the type when no type is known
	public static final TempColumnType TEMP_TYPE = new TempColumnType(UnknownType.INSTANCE);
	
	private final Type actual;
	
	public TempColumnType(Type given) {
		actual = given;
	}
	
	@Override
	public boolean comparableForDistribution(Type t) {
		return true;
	}
	
	@Override
	public NativeType getBaseType() {
		return actual.getBaseType();
	}

	@Override
	public MysqlNativeType getMysqlType() {
		return actual.getMysqlType();
	}

	@Override
	public boolean isUnsigned() {
		return actual.isUnsigned();
	}

	@Override
	public boolean isZeroFill() {
		return actual.isZeroFill();
	}

	@Override
	public boolean isBinaryText() {
		return actual.isBinaryText();
	}

	@Override
	public int getSize() {
		return actual.getSize();
	}

	@Override
	public boolean hasSize() {
		return actual.hasSize();
	}

	@Override
	public boolean hasPrecisionAndScale() {
		return actual.hasPrecisionAndScale();
	}

	@Override
	public int getPrecision() {
		return actual.getPrecision();
	}

	@Override
	public int getScale() {
		return actual.getScale();
	}

	@Override
	public UnqualifiedName getCharset() {
		return actual.getCharset();
	}

	@Override
	public UnqualifiedName getCollation() {
		return actual.getCollation();
	}

	@Override
	public Integer getIndexSize() {
		return actual.getIndexSize();
	}

	@Override
	public TextType asTextType() {
		return actual.asTextType();
	}

	@Override
	public String getComparison() {
		return actual.getComparison();
	}

	@Override
	public boolean declUsesSizing() {
		return actual.declUsesSizing();
	}

	@Override
	public boolean isSerialPlaceholder() {
		return actual.isSerialPlaceholder();
	}

	@Override
	public void addColumnTypeModifiers(UserColumn uc) {
		actual.addColumnTypeModifiers(uc);
	}

	@Override
	public void addColumnTypeModifiers(CatalogColumnEntity cce)
			throws PEException {
		actual.addColumnTypeModifiers(cce);
	}

	@Override
	public String getTypeName() {
		return actual.getTypeName();
	}

	@Override
	public Integer getDataType() {
		return actual.getDataType();
	}

	@Override
	public String getName() {
		return actual.getName();
	}

	@Override
	public boolean mustParameterize() {
		return actual.mustParameterize();
	}

	@Override
	public boolean isBinaryType() {
		return actual.isBinaryType();
	}

	@Override
	public boolean supportsDefaultValue() {
		return actual.supportsDefaultValue();
	}

	@Override
	public boolean isStringType() {
		return actual.isStringType();
	}

	@Override
	public boolean isFloatType() {
		return actual.isFloatType();
	}

	@Override
	public boolean isNumericType() {
		return actual.isNumericType();
	}

	@Override
	public boolean isDecimalType() {
		return actual.isDecimalType();
	}

	@Override
	public boolean isIntegralType() {
		return actual.isIntegralType();
	}

	@Override
	public boolean isBitType() {
		return actual.isBitType();
	}

	@Override
	public boolean isTimestampType() {
		return actual.isTimestampType();
	}

	@Override
	public boolean asKeyRequiresPrefix() {
		return actual.asKeyRequiresPrefix();
	}

	@Override
	public LiteralExpression getZeroValueLiteral() {
		return actual.getZeroValueLiteral();
	}

	@Override
	public Type normalize() {
		return actual;
	}

	@Override
	public boolean isAcceptableColumnTypeForRangeType(Type columnType) {
		return actual.isAcceptableColumnTypeForRangeType(columnType);
	}

	@Override
	public boolean isAcceptableRangeType() {
		return actual.isAcceptableRangeType();
	}

	@Override
	public boolean isUnknown() {
		return actual.isUnknown();
	}

	@Override
	public TextType toTextType() {
		return actual.toTextType();
	}
}
