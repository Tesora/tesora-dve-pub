package com.tesora.dve.comms.client.messages;

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

import java.io.Serializable;

import com.tesora.dve.exceptions.PEException;

public class ErrorInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	protected int errorCode = 99; // TEMP
	protected String errorMsg = null;
	protected PEException exception;

	public ErrorInfo() {
	}

	public ErrorInfo(PEException e) {
		setException(e);
	}

	public int getErrorCode() {
		return errorCode;
	}

	public String getErrorMsg() {
		if(errorMsg == null) 
			errorMsg = getRootException().getMessage();
			
		return errorMsg;
	}

	public PEException getException() {
		return this.exception;
	}

	public void setException(PEException e) {
		this.exception = e;
	}

	public boolean hasException() {
		return this.exception != null;
	}

	public Throwable getRootException() {
		Throwable root, lastEx = exception;
		for (root = exception; (root = root.getCause()) != null;) {
			lastEx = root;
		}
		return lastEx;
	}

	@Override
	public String toString() {
		return errorCode + " : " + getErrorMsg();
	}
}
