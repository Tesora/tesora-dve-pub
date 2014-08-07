package com.tesora.dve.groupmanager;

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

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.server.global.HostService;

public class GlobalVariableInvalidationMessage extends GroupMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5131350016956586338L;

	@Override
	void execute(HostService hostService) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MessageType getMessageType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		// TODO Auto-generated method stub
		return null;
	}

}
