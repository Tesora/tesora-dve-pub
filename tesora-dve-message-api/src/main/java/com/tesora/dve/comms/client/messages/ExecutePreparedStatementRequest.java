// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import java.util.List;

public class ExecutePreparedStatementRequest extends StatementBasedRequest {

	private static final long serialVersionUID = 1L;
	private final List<String> values;
	
	public ExecutePreparedStatementRequest(String stmtID, List<String> params) {
		super(stmtID);
		values = params;
	}
	
	@Override
	public MessageType getMessageType() {
		return MessageType.EXECUTE_PREPARED_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	public List<String> getValues() {
		return values;
	}
	
}
