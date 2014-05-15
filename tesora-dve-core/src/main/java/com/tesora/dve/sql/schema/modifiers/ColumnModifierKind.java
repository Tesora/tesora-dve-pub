// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

public enum ColumnModifierKind {
	
	NULLABLE("NULL",false),
	NOT_NULLABLE("NOT NULL",false),
	DEFAULTVALUE("DEFAULT",false),
	AUTOINCREMENT("AUTOINCREMENT",false),
	ONUPDATE("ON UPDATE",true),
	// front end only modifier, used to handle inline key decls
	INLINE_KEY("unused",false);
	
	private final String sql;
	private final boolean storeAsTypeModifier;
	
	private ColumnModifierKind(String sql, boolean fake) {
		this.sql = sql;
		this.storeAsTypeModifier = fake;
	}

	public String getSQL() { return this.sql; }
	
	public boolean isStoreAsTypeModifier() {
		return storeAsTypeModifier;
	}
	
}