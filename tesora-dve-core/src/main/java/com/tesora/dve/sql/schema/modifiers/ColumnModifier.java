// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

public class ColumnModifier {

	private ColumnModifierKind variety;
	
	public ColumnModifier(ColumnModifierKind kind) {
		variety = kind;
	}
	
	public ColumnModifierKind getTag() { return this.variety; }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((variety == null) ? 0 : variety.hashCode());
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
		ColumnModifier other = (ColumnModifier) obj;
		if (variety != other.variety)
			return false;
		return true;
	}

}
