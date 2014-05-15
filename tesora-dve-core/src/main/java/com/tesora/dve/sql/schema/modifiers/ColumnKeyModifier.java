// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.common.catalog.ConstraintType;

public class ColumnKeyModifier extends ColumnModifier {

	// if null, just a regular key
	private final ConstraintType constraint;
	
	public ColumnKeyModifier(ConstraintType ct) {
		super(ColumnModifierKind.INLINE_KEY);
		constraint = ct;
	}

	public ConstraintType getConstraint() {
		return constraint;
	}
	
}
