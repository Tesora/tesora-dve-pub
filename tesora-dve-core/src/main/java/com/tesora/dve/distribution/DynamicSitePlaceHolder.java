// OS_STATUS: public
package com.tesora.dve.distribution;

import java.util.Map;

import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public class DynamicSitePlaceHolder implements StorageSite {
	
	int position;
	int size;


	public DynamicSitePlaceHolder(int position, int size) {
		super();
		this.position = position;
		this.size = size;
	}

	@Override
	public String getName() {
		return new StringBuffer("Dynamic(").append(position).append("/").append(size).append(")").toString();
	}

	@Override
	public PersistentSite getRecoverableSite(CatalogDAO c) {
		return null;
	}

	@Override
	public Worker pickWorker(Map<StorageSite, Worker> workerMap) throws PEException {
		return workerMap.values().toArray(new Worker[]{})[position % workerMap.size()];
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + position;
		result = prime * result + size;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DynamicSitePlaceHolder other = (DynamicSitePlaceHolder) obj;
		if (position != other.position)
			return false;
		if (size != other.size)
			return false;
		return true;
	}

	@Override
	public String getMasterUrl() {
		return null;
	}

	@Override
	public void annotateStatistics(LogSiteStatisticRequest sNotice) {
	}

	@Override
	public void incrementUsageCount() {
	}

	public int decrementUsageCount() {
		return 0;
	}

	@Override
	public Worker createWorker(UserAuthentication auth) throws PEException {
		return null;
	}

	@Override
	public void onSiteFailure(CatalogDAO c) {
		throw new PECodingException("DynamicSitePlaceHolder should never receive onSiteFailure");
	}

	@Override
	public String getInstanceIdentifier() {
		return getName();
	}

	@Override
	public int getMasterInstanceId() {
		return 0;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "{position=" + position +"/size=" + size +"}";
	}
	
	@Override
	public boolean supportsTransactions() {
		return false;
	}

	@Override
	public boolean hasDatabase(UserVisibleDatabase ctxDB) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setHasDatabase(UserVisibleDatabase ctxDB) throws PEException {
		// TODO Auto-generated method stub
		
	}
}
