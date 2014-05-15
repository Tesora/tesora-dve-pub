// OS_STATUS: public
package com.tesora.dve.exceptions;

import java.sql.SQLException;

public class PESQLStateException extends PESQLException {
    private static final long serialVersionUID = 1L;

    public static final int MYSQL_ERROR_DEADLOCK = 1213;
    public static final int MYSQL_ERROR_XA_RBDEADLOCK = 1614;

    private int errorNumber;
	private String sqlState;
	private String errorMsg;

	public PESQLStateException(int errorNumber, String sqlState, String errorMsg) {
		super("("+errorNumber+": "+sqlState+") "+errorMsg);
		this.errorNumber = errorNumber;
		this.sqlState = sqlState;
		this.errorMsg = errorMsg;
	}

	public PESQLStateException(SQLException e) {
		this(e.getErrorCode(), e.getSQLState(), e.getMessage());
	}

	public int getErrorNumber() {
		return errorNumber;
	}

	public String getSqlState() {
		return sqlState;
	}

	public String getErrorMsg() {
		return errorMsg;
	}


    public boolean isXAFailed() {
        return (this.errorNumber == MYSQL_ERROR_DEADLOCK || this.errorNumber == MYSQL_ERROR_XA_RBDEADLOCK);
    }
}
