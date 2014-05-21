package com.tesora.dve.exceptions;

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

public class PESQLStateException extends PESQLException {
    private static final long serialVersionUID = 1L;

    public static final int MYSQL_ERROR_DEADLOCK = 1213;
    public static final int MYSQL_ERROR_XA_RBDEADLOCK = 1614;

    private int errorNumber;
	private String sqlState;
	private String errorMsg;

	public PESQLStateException(int errorNumber, String sqlState, String errorMsg) {
		super("("+errorNumber+": "+sqlState+") "+errorMsg);
		this.errorNumber = errorNumber;
		this.sqlState = sqlState;
		this.errorMsg = errorMsg;
	}

	public PESQLStateException(SQLException e) {
		this(e.getErrorCode(), e.getSQLState(), e.getMessage());
	}

	public int getErrorNumber() {
		return errorNumber;
	}

	public String getSqlState() {
		return sqlState;
	}

	public String getErrorMsg() {
		return errorMsg;
	}


    public boolean isXAFailed() {
        return (this.errorNumber == MYSQL_ERROR_DEADLOCK || this.errorNumber == MYSQL_ERROR_XA_RBDEADLOCK);
    }
}
