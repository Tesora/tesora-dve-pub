// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import com.tesora.dve.resultset.ResultChunk;

public class FetchResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;

	private transient String from;
	
	protected ResultChunk resultChunk;
	protected boolean noMoreData = false;

	public FetchResponse() {
	}

	public void setResultChunk(ResultChunk resultChunk) {
		this.resultChunk = resultChunk;
	}

	public ResultChunk getResultChunk() {
		return this.resultChunk;
	}

	public boolean noMoreData() {
		return noMoreData;
	}

	public void setNoMoreData(boolean noMoreData) {
		this.noMoreData = noMoreData;
		
		if(!noMoreData)
			success();
	}

	@Override
	public boolean isOK() {
		return super.isOK() || noMoreData;
	}
	
	public void setNoMoreData() {
		setNoMoreData(true);
	}
	
	public FetchResponse from(String from) {
		this.from = from;
		return this;
	}
	
	public String from() {
		return from;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.FETCH_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}
}
