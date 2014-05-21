// OS_STATUS: public
package com.tesora.dve.externalservice;

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


import java.net.InetSocketAddress;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupMembershipListener.MembershipEventType;

public interface ExternalServicePlugin {
	void initialize(ExternalServiceContext ctxt) throws PEException;

	void start() throws PEException;
	
	void stop();
	
	boolean isStarted(); 
	
	String status() throws PEException;
	
	String getName() throws PEException;

	String getPlugin() throws PEException;

	void close();

	void reload() throws PEException;
	
	void restart() throws PEException;

	boolean denyServiceStart(ExternalServiceContext ctxt) throws PEException;
	
	void handleGroupMembershipEvent(MembershipEventType eventType, InetSocketAddress inetSocketAddress);
}
