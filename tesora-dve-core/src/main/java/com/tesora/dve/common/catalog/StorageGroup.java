// OS_STATUS: public
package com.tesora.dve.common.catalog;

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

import java.io.Serializable;
import java.util.Collection;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.worker.WorkerManager;


public interface StorageGroup extends Serializable {

	enum GroupScale { AGGREGATE, SMALL, MEDIUM, LARGE, NONE }

	public String getName();

	@Override
	public String toString();
	
	public int sizeForProvisioning() throws PEException;

	public void provisionGetWorkerRequest(GetWorkerRequest getWorkerRequest) throws PEException;

	public void returnWorkerSites(WorkerManager workerManager, Collection<? extends StorageSite> groupSites) throws PEException;

	public boolean isTemporaryGroup();
	
	public int getId();
}
