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
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.EventLoopGroup;

public class MasterMasterWorker extends Worker {

	public static class Factory implements Worker.Factory {
		@Override
		public Worker newWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop)
				throws PEException {
			return new MasterMasterWorker(auth, additionalConnInfo, site, preferredEventLoop);
		}

		@Override
		public void onSiteFailure(StorageSite site) throws PEException {
			// We know a MasterMasterWorker can only work on a PersistentSite, so to 
			// avoid implementing empty methods everywhere, we'll cast
			((PersistentSite)site).ejectMaster();
		}

		@Override
		public String getInstanceIdentifier(StorageSite site, ISiteInstance instance) {
			return site.getName();
		}
	}

	public static final String HA_TYPE = "MasterMaster";

	public MasterMasterWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop)
			throws PEException {
		super(auth, additionalConnInfo, site, preferredEventLoop);
	}

	@Override
	public WorkerConnection getConnection(StorageSite site, AdditionalConnectionInfo additionalConnInfo, UserAuthentication auth, EventLoopGroup preferredEventLoop) {
		return new MasterMasterConnection(auth, additionalConnInfo, site, preferredEventLoop);
	}

    @Override
    protected void statementHadCommFailure() {
        this.processCommunicationFailure();
    }
}
