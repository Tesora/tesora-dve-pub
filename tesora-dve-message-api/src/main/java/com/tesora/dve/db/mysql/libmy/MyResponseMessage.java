// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

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
