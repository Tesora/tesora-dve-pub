// OS_STATUS: public
package com.tesora.dve.sql.node;

// migration exceptions are used to indicate code paths that
// haven't been migrated to whatever the new idiom is
public class MigrationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MigrationException() {
		// TODO Auto-generated constructor stub
	}

	public MigrationException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public MigrationException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public MigrationException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
