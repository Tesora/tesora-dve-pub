// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
