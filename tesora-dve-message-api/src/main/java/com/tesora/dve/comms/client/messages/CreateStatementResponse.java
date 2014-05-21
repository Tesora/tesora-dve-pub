// OS_STATUS: public
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

public class CreateStatementResponse extends ResponseMessage {

	private static final long serialVersionUID = 8312275422824972802L;

	protected String statementID;

	public CreateStatementResponse(String statementId) {
		setStatementId(statementId);
	}

	public CreateStatementResponse() {
	}

	public void setStatementId(String stmtId) {
		this.statementID = stmtId;
	}

	public String getStatementId() {
		return statementID;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.CREATE_STATEMENT_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
