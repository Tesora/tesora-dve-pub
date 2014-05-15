// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public abstract class MetadataRequest extends StatementBasedRequest {

	public MetadataRequest() {
	}

	public MetadataRequest(String stmtId) {
		super(stmtId);
	}

	private static final long serialVersionUID = 1L;

}
