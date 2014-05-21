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