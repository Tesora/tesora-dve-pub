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

public abstract class ErrorCodeFormatter {

	protected final ErrorCode[] handledCodes;
	protected final int myErrorCode;
	protected final String sqlState;
	// for the most part, I think a format string will work
	protected final String format;
	protected final String verboseFormat;
	
	public ErrorCodeFormatter(ErrorCode ec, String format, int mysqlErrorCode, String state) {
		this.handledCodes = new ErrorCode[] { ec };
		this.myErrorCode = mysqlErrorCode;
		this.sqlState = state;
		this.format = format;
		this.verboseFormat = format + " (at %s)";
	}

	protected String formatInternal(Object[] params, boolean verbose) {
		return String.format(verbose ? verboseFormat : format, params);
	}
	
	public final String format(Object[] params, StackTraceElement location) {
		if (location != null) {
			Object[] nps = new Object[params.length + 1];
			System.arraycopy(params, 0, nps, 0, params.length);
			nps[params.length] = location.toString();
			return formatInternal(nps,true);
		}
		return formatInternal(params,false);
	}
	
	public FormattedErrorInfo buildResponse(Object[] params, StackTraceElement location) {
		return new FormattedErrorInfo(myErrorCode, sqlState, format(params, location));
	}
	
	public SQLException buildException(Object[] params, StackTraceElement location) {
		return new SQLException(format(params,location),sqlState,myErrorCode);
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
