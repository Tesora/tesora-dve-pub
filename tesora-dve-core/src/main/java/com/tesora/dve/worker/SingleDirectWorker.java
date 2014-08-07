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

import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.EventLoopGroup;

public class SingleDirectWorker extends Worker {
	
//	private Logger logger = Logger.getLogger(SingleJDBCWorker.class);

	public static class Factory implements Worker.Factory {
		@Override
		public Worker newWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop)
				throws PEException {
			return new SingleDirectWorker(auth, additionalConnInfo, site, preferredEventLoop);
		}

		@Override
		public void onSiteFailure(StorageSite site) {
		}

		@Override
		public String getInstanceIdentifier(StorageSite site,
				ISiteInstance instance) {
			return site.getName();
		}
	}

	public static final String HA_TYPE = "Single";

	protected SingleDirectWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop) throws PEException {
		super(auth, additionalConnInfo, site, preferredEventLoop);
	}

	@Override
	public WorkerConnection getConnection(StorageSite site, AdditionalConnectionInfo additionalConnInfo, UserAuthentication auth, EventLoopGroup preferredEventLoop) {
		SingleDirectConnection wConnection = new SingleDirectConnection(auth, additionalConnInfo, site, preferredEventLoop);
//		if (logger.isDebugEnabled())
//			try {
//				logger.debug("Worker " + getName() + " gets JDBC connection " + wConnection.getConnection().toString());
//			} catch (PESQLException e) {} 
		return wConnection;
	}
}
