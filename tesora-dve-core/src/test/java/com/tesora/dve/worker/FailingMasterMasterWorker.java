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
import com.tesora.dve.worker.MasterMasterWorker;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerConnection;

public class FailingMasterMasterWorker extends MasterMasterWorker {

	public static class Factory extends MasterMasterWorker.Factory {
		@Override
		public Worker newWorker(UserAuthentication auth, StorageSite site)
				throws PEException {
			return new FailingMasterMasterWorker(auth, site);
		}
		@Override
		public String getInstanceIdentifier(StorageSite site, ISiteInstance instance) {
			return new StringBuffer(site.getName()).append("_").append(instance.getId()).toString();
		}
	}

	public static final String HA_TYPE = "FailingMasterMaster";

	public FailingMasterMasterWorker(UserAuthentication auth, StorageSite site)
			throws PEException {
		super(auth, site);
	}

	@Override
	public WorkerConnection getConnection(StorageSite site, UserAuthentication auth) {
		return new FailingMasterMasterConnection(auth, site);
	}

}
