package com.tesora.dve.sql.statement.session;

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

import java.sql.Connection;
import java.util.Locale;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;

public class SetTransactionIsolationExpression extends SetExpression {

	private VariableScope scope;
	private IsolationLevel level;
	
	public SetTransactionIsolationExpression(IsolationLevel level, VariableScopeKind scope) {
		super();
		this.scope = new VariableScope(scope == null ? VariableScopeKind.SESSION : scope);
		this.level = level;
	}
	
	public VariableScope getScope() {
		return scope;
	}
	
	public IsolationLevel getLevel() {
		return level;
	}
	
	
	@Override
	public Kind getKind() {
		return Kind.TRANSACTION_ISOLATION;
	}
	
	public enum IsolationLevel {
		READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED, "READ UNCOMMITTED"),
		READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED, "READ COMMITTED"),
		REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ, "REPEATABLE READ"),
		SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE, "SERIALIZABLE");
		
		private final int jdbcIsolationLevel;
		private final String sql;
				
		private IsolationLevel(int il, String sql) {
			jdbcIsolationLevel = il;
			this.sql = sql;
		}
		
		public int getJdbcIsolationLevel() {
			return jdbcIsolationLevel;
		}
		
		public String getSQL() {
			return this.sql;
		}
		
		public String getHostSQL() throws PEException {
            return Singletons.require(HostService.class).getDBNative().convertTransactionIsolationLevel(jdbcIsolationLevel);
		}
		
		public static IsolationLevel convert(String in) {
			if (in == null) return null;
			String uc = in.toUpperCase(Locale.ENGLISH);
			for(IsolationLevel il : IsolationLevel.values()) {
				if (uc.equals(il.getSQL()))
					return il;
			}
			return null;
		}
	}
}
