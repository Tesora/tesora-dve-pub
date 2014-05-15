// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

public class StringTypeModifier extends TypeModifier {

	protected String value;
	
	public StringTypeModifier(TypeModifierKind tmk, String v) {
		super(tmk);
		value = v;
	}

	public String getValue() {
		return value;
	}
	
}
