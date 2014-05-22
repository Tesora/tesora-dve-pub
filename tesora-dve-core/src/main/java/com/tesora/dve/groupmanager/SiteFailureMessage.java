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

import com.tesora.dve.server.global.HostService;
import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;

public class SiteFailureMessage extends GroupMessage {

	private static final long serialVersionUID = 1L;
	
	static Logger logger = Logger.getLogger(SiteFailureMessage.class);

	int siteInstanceId;

	public int getSiteInstanceId() {
		return siteInstanceId;
	}

	public SiteFailureMessage(int siteInstanceId) {
		this.siteInstanceId = siteInstanceId;
	}

	@Override
	void execute(HostService hostService) {
		try {
			PerHostConnectionManager.INSTANCE.sendToAllConnections(this);
			SchemaSourceFactory.onModification(CacheInvalidationRecord.GLOBAL);
		} catch (PEException e) {
			logger.warn("Unable to execute site failover for site instance " + siteInstanceId, e);
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(" + siteInstanceId + ")";
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		return null;
	}

}
