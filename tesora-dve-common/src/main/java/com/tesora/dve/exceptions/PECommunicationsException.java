// OS_STATUS: public
package com.tesora.dve.exceptions;

import java.sql.SQLException;


public class PECommunicationsException extends PESQLException {

	private static final long serialVersionUID = 1L;
	public static final Exception INSTANCE = new PECommunicationsException();

	protected PECommunicationsException() {
		super();
	}
	
	public PECommunicationsException(String m) {
		super(m);
	}
	
	public PECommunicationsException(String m, Exception e) {
		super(m, e);
	}

	public PECommunicationsException(SQLException e) {
		super(e);
	}

}
