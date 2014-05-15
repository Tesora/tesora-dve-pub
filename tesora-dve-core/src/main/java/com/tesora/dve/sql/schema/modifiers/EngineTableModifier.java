// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.schema.SchemaContext;

public class EngineTableModifier extends TableModifier {

	public enum EngineTag {
		
		INNODB("InnoDB"),
		MEMORY("MEMORY"),
		MYISAM("MyISAM");
		
		
		private final String sql;
		
		private EngineTag(String sql) {
			this.sql = sql;
		}
		
		public String getSQL() {
			return sql;
		}
		
		public static EngineTag findEngine(String text) {
			for(EngineTag e : EngineTag.values()) {
				if (e.getSQL().equalsIgnoreCase(text))
					return e;
			}
			return null;
		}
		
	}
	
	private EngineTag engine;
	
	public EngineTableModifier(EngineTag et) {
		super();
		engine = et;
	}
	
	public EngineTag getEngine() {
		return engine;
	}

	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		buf.append("ENGINE=").append(engine.getSQL());
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.ENGINE;
	}
	
	public boolean isInnoDB() {
		return engine == EngineTag.INNODB;
	}

	public boolean isMyISAM() {
		return engine == EngineTag.MYISAM;
	}
	
	public String getPersistent() {
		return engine.getSQL();
	}
}
