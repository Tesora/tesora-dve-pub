package com.tesora.dve.siteprovider;

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

import java.util.Comparator;
import java.util.Map;

import io.netty.channel.EventLoopGroup;
import org.apache.log4j.Logger;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.server.statistics.SiteStatKey.SiteType;
import com.tesora.dve.sql.transform.execution.AdhocCatalogEntity;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.worker.AdditionalConnectionInfo;
import com.tesora.dve.worker.SingleDirectWorker;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public class DynamicSiteInfo extends AbstractDynamicSiteInfo implements StorageSite {

	private static Logger logger = Logger.getLogger(DynamicSiteInfo.class);
	private long lastQueryTime = System.currentTimeMillis();

	static final Comparator<DynamicSiteInfo> LOAD_AND_TIME_ORDER = new Comparator<DynamicSiteInfo>() {
		@Override
		public int compare(DynamicSiteInfo o1, DynamicSiteInfo o2) {
			int diff = o1.currentQueries.get() - o2.currentQueries.get();
			return (diff != 0) ? diff : (int) (o1.lastQueryTime - o2.lastQueryTime);
		}
	};
	
	private UserAuthentication siteAuth;

	protected DynamicSiteInfo() {
	}

	public DynamicSiteInfo(String provider, String pool, String name, String url, String user, String password, int maxQueries) {
		super(provider, pool, name, url, user, password, maxQueries);
		
		siteAuth = new UserAuthentication(user, password, false);
	}

	// ------------------------------------------------------------------------
	// The following methods are from the StorageSite interface

	@Override
	public String getName() {
		return getFullName();
	}

	@Override
	public String getMasterUrl() {
		return getUrl();
	}

	@Override
	public PersistentSite getRecoverableSite(CatalogDAO c) {
		return null;
	}

	@Override
	public Worker pickWorker(Map<StorageSite, Worker> workerMap) throws PEException {
		return workerMap.get(this);
	}

	@Override
	public void annotateStatistics(LogSiteStatisticRequest sNotice) {
		sNotice.setSiteDetails(getName(), SiteType.DYNAMIC);
	}

	// ------------------------------------------------------------------------

	@Override
	public void incrementUsageCount() {
		currentQueries.incrementAndGet();
		totalQueries.incrementAndGet();

		lastQueryTime = System.currentTimeMillis();
	}

	public void decrementUsageCount(boolean unused) {
		currentQueries.decrementAndGet();
		if (unused)
			totalQueries.decrementAndGet();
	}

	public boolean isAvailable() {
		return state == DynamicSiteStatus.ONLINE;
	}

	public void markForRemoval() {
		setState(DynamicSiteStatus.DELETED, true);
	}

	public boolean isMarkedForRemoval() {
		return state == DynamicSiteStatus.DELETED;
	}

	public boolean isReadyForRemoval() {
		return isMarkedForRemoval() && (currentQueries.get() == 0);
	}

	public CatalogEntity toCatalogEntity(String providerName, String poolName) {
		ListOfPairs<String, Object> pairs = new ListOfPairs<String, Object>();
		pairs.add(ShowSchema.GroupProviderSites.SITE_NAME, getFullName());
		pairs.add(ShowSchema.GroupProviderSites.PROVIDER, providerName);
		pairs.add(ShowSchema.GroupProviderSites.POOL, poolName);
		pairs.add(ShowSchema.GroupProviderSites.NAME, getShortName());
		pairs.add(ShowSchema.GroupProviderSites.URL, getUrl());
		pairs.add(ShowSchema.GroupProviderSites.MAX_QUERIES, getMaxQueries());
		pairs.add(ShowSchema.GroupProviderSites.CURRENT_QUERIES, getCurrentQueries());
		pairs.add(ShowSchema.GroupProviderSites.TOTAL_QUERIES, getTotalQueries());
		pairs.add(ShowSchema.GroupProviderSites.STATUS, getState().toString());
		pairs.add(ShowSchema.GroupProviderSites.TIMESTAMP, getTimestamp());
		return new AdhocCatalogEntity(pairs);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("name=" + getShortName());
		sb.append(", url=" + getUrl());
		sb.append(", user=" + getUser());
		sb.append(", maxQueries=" + getMaxQueries());
		sb.append(", currentQueries=" + getCurrentQueries());
		sb.append(", state=" + getState().toString());
		return sb.toString();
	}

	private static SingleDirectWorker.Factory factory = new SingleDirectWorker.Factory();

	@Override
	public Worker createWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, EventLoopGroup preferredEventLoop) throws PEException {
		// For dynamic sites we will use the site credentials rather than the
		// ones passed in
		return factory.newWorker(siteAuth, additionalConnInfo, this, preferredEventLoop);
	}

	@Override
	public void onSiteFailure(CatalogDAO c) {
		logger.error("Failure on dynamic site " + getShortName() + " (url= " + getMasterUrl() + ")");
		this.setState(DynamicSiteStatus.FAILED, true);
	}

	@Override
	public String getInstanceIdentifier() {
		return getName();
	}

	@Override
	public int getMasterInstanceId() {
		return 0;
	}


}
