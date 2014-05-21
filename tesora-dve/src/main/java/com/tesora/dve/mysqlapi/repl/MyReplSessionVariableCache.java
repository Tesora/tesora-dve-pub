// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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

import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.primitives.UnsignedLong;
import com.tesora.dve.dbc.ServerDBConnection;
import com.tesora.dve.mysqlapi.repl.messages.MyIntvarLogEvent.MyIntvarEventVariableType;
import com.tesora.dve.sql.util.Pair;

public class MyReplSessionVariableCache {

	private static final String INSERT_ID_VAR = "insert_id";
	private static final String LAST_INSERT_ID_VAR = "last_insert_id";
	private static final String RAND_SEED1_VAR = "rand_seed1";
	private static final String RAND_SEED2_VAR = "rand_seed2";
	
	// Rotate Log variable
	String masterLogFile = null;
	Long masterLogPosition = null;
	
	// Intvar variables
	Byte intVarType = null;
	UnsignedLong intVarValue = null;
	
	// Rand variables
	UnsignedLong seed1 = null;
	UnsignedLong seed2 = null;
	
	public void setAllSessionVariableValues(ServerDBConnection conn) throws SQLException {
		// don't set the variable directly use the new PE variable instead for IntVar event
//		setIntVarVariable(conn);
		setRandVariable(conn);
	}

	public void clearAllSessionVariables() {
		clearIntVarValue();
		clearRandValue();
	}

	public void setRotateLogValue(String masterLogFile) {
		this.masterLogFile = masterLogFile;
	}

	public String getRotateLogValue() {
		return this.masterLogFile;
	}

	public void setRotateLogPositionValue(Long masterLogPosition) {
		this.masterLogPosition = masterLogPosition;
	}

	public Long getRotateLogPositionValue() {
		return this.masterLogPosition;
	}

	public void setIntVarValue(	Byte variableType, UnsignedLong variableValue) {
		this.intVarType = variableType;
		this.intVarValue = variableValue;
	}
	
	public Pair<Byte, UnsignedLong> getIntVarValue() {
		return new Pair<Byte, UnsignedLong>(this.intVarType, this.intVarValue);
	}
	
	public void clearIntVarValue() {
		this.intVarType = null;
		this.intVarValue = null;
	}
	
	public void setIntVarVariable(Statement stmt) throws SQLException {
		if ((this.intVarType == null) || (this.intVarValue == null)) {
			// must have some values to set
			return;
		}
		
		if (MyIntvarEventVariableType.fromByte(intVarType) == MyIntvarEventVariableType.LAST_INSERT_ID_EVENT) {
			stmt.executeUpdate(buildSetSessionStatement(LAST_INSERT_ID_VAR, this.intVarValue));
		} else {
			stmt.executeUpdate(buildSetSessionStatement(INSERT_ID_VAR, this.intVarValue));
		}
	}
	
	public void setRandValue(UnsignedLong seed1, UnsignedLong seed2) {
		this.seed1 = seed1;
		this.seed2 = seed2;
	}

	public void getRandValue(UnsignedLong seed1, UnsignedLong seed2) {
		seed1 = this.seed1;
		seed2 = this.seed2;
	}

	public void clearRandValue() {
		this.seed1 = null;
		this.seed2 = null;
	}

	public void setRandVariable(ServerDBConnection conn) throws SQLException {
		if ((this.seed1 == null) || (this.seed2 == null)) {
			// must have some values to set
			return;
		}
		
		conn.executeUpdate(buildSetSessionStatement(RAND_SEED1_VAR, this.seed1));
		conn.executeUpdate(buildSetSessionStatement(RAND_SEED2_VAR, this.seed2));
	}
	
	String buildSetSessionStatement(String variableName, Object variableValue) {
		return "SET " + variableName + "=" + variableValue.toString();
	}
}
