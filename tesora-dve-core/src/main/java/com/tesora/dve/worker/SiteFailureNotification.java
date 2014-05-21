// OS_STATUS: public
package com.tesora.dve.worker;

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

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.NotificationManager;
import com.tesora.dve.server.connectionmanager.NotificationManagerRequest;
import com.tesora.dve.worker.agent.Envelope;

public class SiteFailureNotification extends NotificationManagerRequest {

	private Logger logger = Logger.getLogger(SiteFailureNotification.class);

	private static final long serialVersionUID = 1L;
	private StorageSite site;

	public SiteFailureNotification(StorageSite site) {
		this.site = site;
	}

	public StorageSite getSite() {
		return site;
	}

	@Override
	public ResponseMessage executeRequest(Envelope e, NotificationManager nm)
			throws PEException {
		try {
			nm.onSiteFailure(site);
		} catch (Throwable t) {
			logger.error("Error processing site failure notification", t);
		}
		return null;
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
