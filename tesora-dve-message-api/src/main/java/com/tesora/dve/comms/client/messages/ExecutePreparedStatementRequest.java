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
