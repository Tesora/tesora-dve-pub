// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

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
