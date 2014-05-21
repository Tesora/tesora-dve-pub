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

public abstract class StatementBasedRequest extends RequestMessage {

	private static final long serialVersionUID = 1L;

	protected String statementID;
	
	protected StatementBasedRequest() {
	};

	protected StatementBasedRequest(String stmtId) {
		this.setStatementId(stmtId);
	}

	public String getStatementId() {
		return this.statementID;
	}

	public void setStatementId(String stmtId) {
		this.statementID = stmtId;
	}
}
