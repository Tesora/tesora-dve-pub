// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class RowFormatTableModifier extends CreateOptionModifier {

	public enum Format {
		
		DEFAULT("DEFAULT"),
		DYNAMIC("DYNAMIC"),
		FIXED("FIXED"),
		COMPRESSED("COMPRESSED"),
		REDUNDANT("REDUNDANT"),
		COMPACT("COMPACT");
		
		private final String sql;
		
		private Format(String sql) {
			this.sql = sql;
		}
		
		public String getSQL() {
			return sql;
		}
		
		public static Format find(String n) {
			for(Format f : Format.values()) {
				if (f.getSQL().equalsIgnoreCase(n))
					return f;
			}
			return null;
		}
	}
	
	private Format format;
	
	public RowFormatTableModifier(UnqualifiedName unq) {
		this(unq.get());
	}
	
	public RowFormatTableModifier(String n) {
		format = Format.find(n);
		if (format == null)
			throw new SchemaException(Pass.SECOND, "Unknown row format: " + n);		
	}
	
	public Format getRowFormat() {
		return format;
	}
	
	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		buf.append("ROW_FORMAT=").append(format.getSQL());
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.ROW_FORMAT;
	}

	@Override
	public String persist() {
		return "row_format=" + format.getSQL();
	}

}
