// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.exceptions.PEException;

public class PEMysqlErrorException extends PEException {

	private static final long serialVersionUID = 1L;

	public PEMysqlErrorException() {
		super("Mysql error was intercepted");
	}

	public PEMysqlErrorException(Exception e) {
		super("Mysql error forwarded to user", e);
	}
}
