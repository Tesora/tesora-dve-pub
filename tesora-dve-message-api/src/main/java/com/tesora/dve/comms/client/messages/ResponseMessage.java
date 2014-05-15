// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public abstract class ResponseMessage extends ClientMessage {
	private static final Logger logger = Logger.getLogger(ResponseMessage.class);

	private static final long serialVersionUID = 1L;

	protected ErrorInfo errorInfo = null;
	protected boolean success = false;

	public ResponseMessage() {
	}

	public ResponseMessage(PEException e) {
		setException(e);
	}

	public ResponseMessage setException(PEException e) {
		if (errorInfo == null)
			errorInfo = new ErrorInfo(e);

		success = false;
		return this;
	}

	public boolean hasException() {
		if (errorInfo == null)
			return false;

		return errorInfo.hasException();
	}

	public PEException getException() {
		if (errorInfo == null)
			return null;

		return errorInfo.getException();
	}

	public ResponseMessage success() {
		success = true;
		errorInfo = null;
		return this;
	}

	public ErrorInfo getError() {
		if (errorInfo == null)
			return new ErrorInfo();

		return errorInfo;
	}

	public boolean isOK() {
		return success;
	}
	
	@Override
	public void marshallMessage() {
		// If there is an exception in the response then we need to marshall it into something 
		// that can be sent to the client. 
		if (errorInfo != null && errorInfo.hasException()) {
			logger.error("Exception relayed to client:", errorInfo.getRootException());
			
			// This will populate the root exception message into the errorInfo
			errorInfo.getErrorMsg();
			
			// and then clear out the original exception
			errorInfo.setException(null);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{").append(getMessageType());
		if (errorInfo != null) {
			builder.append(" errorInfo=").append(errorInfo);
			if (errorInfo.hasException())
				builder.append(", exception=").append(errorInfo.getRootException());
		}
		return builder.toString();
	}
}
