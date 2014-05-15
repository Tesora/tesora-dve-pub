// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;


public class TypeModifier {
	
	protected TypeModifierKind kind;
	
	public TypeModifier(TypeModifierKind tmk) {
		kind = tmk;
	}

	public TypeModifierKind getKind() {
		return kind;
	}
	
}
