// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

public enum TableModifierTag {
	
	// declaration order is the order in which we emit these
	ENGINE,
	AUTOINCREMENT,
	DEFAULT_CHARSET,
	DEFAULT_COLLATION,
	MAX_ROWS(true),
	CHECKSUM(true),
	ROW_FORMAT(true),
	COMMENT;

	private final boolean createOption;
	
	private TableModifierTag(boolean co) {
		createOption = co;
	}
	
	private TableModifierTag() {
		this(false);
	}
	
	public final boolean isCreateOption() {
		return createOption;
	}
	
}