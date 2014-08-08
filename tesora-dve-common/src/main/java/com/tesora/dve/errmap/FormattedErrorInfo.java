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

public class FormattedErrorInfo {

	private final int errorNumber;
	private final String sqlState;
	private final String errorMsg;

	
	public FormattedErrorInfo(int errno, String sqlState, String errMsg) {
		this.errorNumber = errno;
		this.sqlState = sqlState;
		this.errorMsg = errMsg;
	}

	public int getErrNo() {
		return errorNumber;
	}
	
	public String getSQLState() {
		return sqlState;
	}
	
	public String getErrorMessage() {
		return errorMsg;
	}
	
}
