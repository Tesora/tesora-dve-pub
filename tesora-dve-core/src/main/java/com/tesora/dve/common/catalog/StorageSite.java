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

import java.util.Map;

import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;
import io.netty.channel.EventLoopGroup;

public interface StorageSite {

	String getName();

	String getMasterUrl();

	String getInstanceIdentifier();

	/**
	 * getRecoverableSite() returns a site to be included in transaction recovery.  If the site
	 * does not participate in a transaction that needs to be recovered, this method should return
	 * null. (Dynamic sites should always return null)
	 * @return a PersistentSite, or null
	 */
	PersistentSite getRecoverableSite(CatalogDAO c);

	// just copy pickWorker from PersisentSite
	Worker pickWorker(Map<StorageSite, Worker> workerMap) throws PEException;

	void annotateStatistics(LogSiteStatisticRequest sNotice);

	void incrementUsageCount();

	Worker createWorker(UserAuthentication auth, EventLoopGroup preferredEventLoop) throws PEException;

	/**
	 * Called when the notification manager decides that 
	 * a site can no longer be reached
	 * @param site
	 * @return TODO
	 * @throws PEException 
	 */
	void onSiteFailure(CatalogDAO c) throws PEException;

	int getMasterInstanceId();
	
	boolean supportsTransactions();
	
	boolean hasDatabase(UserVisibleDatabase ctxDB);
	
	void setHasDatabase(UserVisibleDatabase ctxDB) throws PEException;
}