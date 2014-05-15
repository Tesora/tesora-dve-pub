// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import com.tesora.dve.resultset.ColumnSet;

public class PrepareResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;

	protected ColumnSet params;
	protected ColumnSet columns;

	public PrepareResponse(ColumnSet params, ColumnSet columns) {
		this.params = params;
		this.columns = columns;
	}
	
	public int getNumColumns() {
		return columns.size();
	}
	
	public int getNumParams() {
		return params.size();
	}
	
	public ColumnSet getParams() {
		return params;
	}

	public void setParams(ColumnSet params) {
		this.params = params;
	}

	public ColumnSet getColumns() {
		return columns;
	}

	public void setColumns(ColumnSet columns) {
		this.columns = columns;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.PREPARE_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}
}