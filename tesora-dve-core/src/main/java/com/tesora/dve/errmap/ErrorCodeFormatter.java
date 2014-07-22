package com.tesora.dve.errmap;

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

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;

public abstract class ErrorCodeFormatter {

	protected final ErrorCode[] handledCodes;
	protected final int myErrorCode;
	protected final String sqlState;
	// for the most part, I think a format string will work
	protected final String format;
	
	public ErrorCodeFormatter(ErrorCode ec, String format, int mysqlErrorCode, String state) {
		this.handledCodes = new ErrorCode[] { ec };
		this.myErrorCode = mysqlErrorCode;
		this.sqlState = state;
		this.format = format;
	}

	public String format(Object[] params) {
		return String.format(format,params);
	}
	
	public MyErrorResponse buildResponse(Object[] params) {
		return new MyErrorResponse(myErrorCode, sqlState, format(params));
	}
	
	public SQLException buildException(Object[] params) {
		return new SQLException(format(params),sqlState,myErrorCode);
	}
	
	public ErrorCode[] getHandledCodes() {
		return handledCodes;
	}
	
	// used in testing
	public int getNativeCode() {
		return myErrorCode;
	}
	
	public String getSQLState() {
		return sqlState;
	}
	
}
