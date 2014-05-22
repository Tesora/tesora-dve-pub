package com.tesora.dve.sql.statement;

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

import java.util.HashMap;

import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.statement.session.SessionSetVariableStatement;

// so, we're being slightly more unforgiving about locking - now we start acquiring
// locks during parsing, which means we need to have information about the stmt we are
// building before it is completely built, so external traits are needed.
public class StatementTraits {

	private final HashMap<String, LockType> lockTypes;
	
	private StatementTraits() {
		lockTypes = buildLockTypes();
	}
	
	private static final StatementTraits traits = new StatementTraits();
	
	public static LockType getLockType(Class<?> c) {
		return traits.lockTypes.get(c.getSimpleName());
	}
	
	public static LockType getLockType(String stmtName) {
		return traits.lockTypes.get(stmtName);
	}
	
	private static HashMap<String, LockType> buildLockTypes() {
		HashMap<String,LockType> out = new HashMap<String,LockType>();
		Class<?>[] wshared = new Class<?>[] {
				DeleteStatement.class,
				UpdateStatement.class,
				TruncateStatement.class,
				ReplaceIntoValuesStatement.class,
				InsertIntoValuesStatement.class,
				InsertIntoSelectStatement.class,
				ReplaceIntoSelectStatement.class
		};
		Class<?>[] rshared = new Class<?>[] {
				UnionStatement.class,
				SelectStatement.class,
				SessionSetVariableStatement.class
		};
		for(Class<?> c : wshared)
			out.put(c.getSimpleName(), LockType.WSHARED);
		for(Class<?> c : rshared)
			out.put(c.getSimpleName(), LockType.RSHARED);
		// these are strictly for parsing
		out.put("select", LockType.RSHARED);
		out.put("insert", LockType.WSHARED);
		out.put("replace", LockType.WSHARED);
		out.put("delete", LockType.WSHARED);
		out.put("update", LockType.WSHARED);
		out.put("truncate", LockType.WSHARED);
		return out;
	}
}
