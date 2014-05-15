// OS_STATUS: public
package com.tesora.dve.common;

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
