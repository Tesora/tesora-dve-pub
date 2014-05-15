// OS_STATUS: public
package com.tesora.dve.sql.schema;

public class FloatSizeTypeAttribute extends SizeTypeAttribute {

	private final int scale;
	private final int precision;
	
	public FloatSizeTypeAttribute(int size, int precision, int scale) {
		super(size, SizeTypeAttributeEnum.FLOAT_TYPE);
		this.scale = scale;
		this.precision = precision;
	}
	
	public int getScale() { return scale; }
	public int getPrecision() { return precision; }
	
}
