package com.tesora.dve.distribution;

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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.AdditionalConnectionInfo;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;
import io.netty.channel.EventLoopGroup;

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
        int mapSize = workerMap.size();
        Set<Map.Entry<StorageSite,Worker>> entrySet = workerMap.entrySet();
        int desiredElement = position % entrySet.size();
        Iterator<Map.Entry<StorageSite,Worker>> entryIterator = entrySet.iterator();

        for (int current = 0;entryIterator.hasNext();current++){
            Map.Entry<StorageSite,Worker> thisEntry = entryIterator.next();
            if (current == desiredElement)
                return thisEntry.getValue();
        }
        throw new PECodingException("Didn't find "+position+" element in "+workerMap);
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
	public Worker createWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, EventLoopGroup preferredEventLoop) throws PEException {
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
