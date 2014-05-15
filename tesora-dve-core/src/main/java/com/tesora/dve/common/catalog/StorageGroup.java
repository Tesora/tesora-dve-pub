// OS_STATUS: public
package com.tesora.dve.common.catalog;

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
