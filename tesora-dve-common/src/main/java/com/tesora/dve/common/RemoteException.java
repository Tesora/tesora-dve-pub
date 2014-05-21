// OS_STATUS: public
package com.tesora.dve.common;

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

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.tesora.dve.exceptions.PEException;

public class RemoteException extends PEException {
	
	static private String hostName;
	{
		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			hostName = "Unknown";
		}
	}

	protected RemoteException() {
		super();
	}
	
	public RemoteException(String agentName, Exception e) {
		super("Exception intercepted on "+hostName+"/"+agentName, e);
	}

	public RemoteException(String agentName, Throwable t) {
		super("Exception intercepted on "+hostName+"/"+agentName, new PEException("Throwable caught", t));
	}

	private static final long serialVersionUID = 1L;
	
	@Override
	public Exception rootCause() {
		Throwable cause = getCause();
		if (cause instanceof RemoteException)
			cause = ((RemoteException) cause).rootCause();
		return (Exception) ((cause == null) ? this : cause);
	}

}
