// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.sql.node.expression.ExpressionNode;

public class DefaultValueModifier extends ColumnModifier {

	protected ExpressionNode defaultValue;
	
	public DefaultValueModifier(ExpressionNode v) {
		super(ColumnModifierKind.DEFAULTVALUE);
		defaultValue = v;
	}
	
	public ExpressionNode getDefaultValue() {
		return defaultValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((defaultValue == null) ? 0 : defaultValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultValueModifier other = (DefaultValueModifier) obj;
		if (defaultValue == null) {
			if (other.defaultValue != null)
				return false;
		} else if (!defaultValue.equals(other.defaultValue))
			return false;
		return true;
	}
	
	
	
}
