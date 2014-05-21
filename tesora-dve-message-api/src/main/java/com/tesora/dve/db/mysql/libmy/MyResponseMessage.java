// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

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

public abstract class MyResponseMessage extends MyMessage {

	private boolean isOK = true;
	// can be used for requests that don't need a response sent
	private boolean sendResponse = true; 
	
	Exception exception;
	boolean hasException = false;
	
	public boolean isOK() {
		return isOK;
	}

	public void setOK(boolean isOK) {
		this.isOK = isOK;
	}

	public Exception getException() {
		return exception;
	}

	public void setException(Exception exception) {
		hasException = true;
		this.exception = exception;
	}

	public boolean hasException() {
		return hasException;
	}

    public boolean isErrorResponse(){
        return false;
    }

	public boolean sendResponse() {
		return sendResponse;
	}

	public void sendResponse(boolean sendResponse) {
		this.sendResponse = sendResponse;
	}

	@Override
	public boolean isMessageTypeEncoded() {
		return false;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(super.toString());
		builder.append(" isOK=" + isOK);
		return builder.toString();
	}
}
