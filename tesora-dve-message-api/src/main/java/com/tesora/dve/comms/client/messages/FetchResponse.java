package com.tesora.dve.comms.client.messages;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
