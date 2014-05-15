// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import java.sql.Connection;
import java.util.Locale;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.VariableScope;

public class SetTransactionIsolationExpression extends SetExpression {

	private VariableScope scope;
	private IsolationLevel level;
	
	public SetTransactionIsolationExpression(IsolationLevel level, VariableScope.VariableKind scope) {
		super();
		this.scope = new VariableScope(scope == null ? VariableScope.VariableKind.SESSION : scope);
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
