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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.DynamicGroupClass;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.IDynamicPolicy;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.siteprovider.SiteProviderContextForProvision;
import com.tesora.dve.siteprovider.SiteProviderPlugin;
import com.tesora.dve.siteprovider.SiteProviderPlugin.SiteProviderFactory;

@SuppressWarnings("serial")
public class DynamicGroup implements StorageGroup {

	static AtomicInteger nextGroupId = new AtomicInteger(0);

	int dynamicGroupId = nextGroupId.incrementAndGet();

	GroupScale scale;
	GroupScale nextScale;

	String name;
	DynamicGroupClass groupClass = null;
	IDynamicPolicy policy = null;

	private final static Map<GroupScale, AtomicLong> counters = new HashMap<GroupScale, AtomicLong>();
	
	static {
		for(GroupScale scale : GroupScale.values()) {
			counters.put(scale, new AtomicLong(0));
		}
	}

	public DynamicGroup(IDynamicPolicy policy, GroupScale scale) throws PEException {
		this(policy);
		doSetScale(scale);
	}

	public DynamicGroup(IDynamicPolicy policy) {
		this.policy = policy;
		setName(this.getClass().getSimpleName());
	}

	private void setName(String simpleName) {
		this.name = simpleName + dynamicGroupId;
	}

	public void setScale(GroupScale scale) throws PEException {
		// If we have been configured for aggregation, we don't want to scale
		// up to multiple sites
		if (scale != GroupScale.AGGREGATE)
			doSetScale(scale);
	}

	public static long getCurrentUsage(GroupScale scale) throws PEException {
		AtomicLong val = counters.get(scale);
		
		if(val == null)
			throw new PEException("Unsupported GroupScale: " + scale.toString());
		
		return val.get();
	}
	
	public static void resetCurrentUsage(GroupScale scale) throws PEException {
		AtomicLong val = counters.get(scale);
		
		if(val == null)
			throw new PEException("Unsupported GroupScale: " + scale.toString());
		
		val.set(0);
	}

	void doSetScale(GroupScale scale) throws PEException {
		if (policy == null)
			throw new PEException("DynamicPolicy is not defined");

		this.scale = scale;
		switch (scale) {
		case AGGREGATE:
			groupClass = policy.getAggregationClass();
			setName(PEConstants.AGGREGATE);
			nextScale = GroupScale.NONE;
			break;
		case SMALL:
			groupClass = policy.getSmallClass();
			setName(DynamicPolicy.SMALL);
			nextScale = GroupScale.MEDIUM;
			break;
		case MEDIUM:
			groupClass = policy.getMediumClass();
			setName(DynamicPolicy.MEDIUM);
			nextScale = GroupScale.LARGE;
			break;
		case LARGE:
			groupClass = policy.getLargeClass();
			setName(DynamicPolicy.LARGE);
			nextScale = GroupScale.NONE;
			break;
		default:
			throw new PEException("Unsupported DynamicGroup scale: " + scale.toString());
		}
		if (groupClass.getCount() == 0)
			upscaleGroup();
	}

	public void upscaleGroup() throws PEException {
		if (nextScale != null)
			doSetScale(nextScale);
		else
			throw new PEException("DynamicGroup cannot be upscaled to a larger group (no larger groups defined)");
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void returnWorkerSites(WorkerManager workerManager, Collection<? extends StorageSite> sites)
			throws PEException {
		String providerType = groupClass.getProvider();
		SiteProviderPlugin siteProvider = SiteProviderFactory.getInstance(providerType);
		SiteProviderContextForProvision ctxt = new SiteProviderContextForProvision(providerType);
		try {
			siteProvider.returnSitesByClass(ctxt, groupClass.getPoolName(), sites);
		} finally {
			ctxt.close();
		}
	}

	@Override
	public void provisionGetWorkerRequest(GetWorkerRequest getWorkerRequest) throws PEException {
		if (groupClass != null) {
			getWorkerRequest.addProvisioningDetails(groupClass.getPoolName(), groupClass.getCount(),
					policy.getStrict());
			
			String providerType = groupClass.getProvider();
			if(providerType == null || providerType.length() == 0)
				throw new PEException("Cannot provision workers - site provider not defined");
			
			SiteProviderPlugin siteProvider = SiteProviderFactory.getInstance(providerType);
			if(siteProvider == null)
				throw new PEException("Cannot provision workers - site provider '" + providerType + "' does not exist");
			
			SiteProviderContextForProvision ctxt = new SiteProviderContextForProvision(providerType);
			try {
				if (siteProvider.isEnabled()) {
					siteProvider.provisionWorkerRequest(ctxt, getWorkerRequest);

					counters.get(scale).incrementAndGet();
				} else
					throw new PEException("Cannot provision workers - site provider '" + siteProvider.getProviderName()
							+ "' not enabled");
			} finally {
				ctxt.close();
			}
		} else
			throw new PEException("Cannot provision a group whose scale has not been set");
	}

	@Override
	public boolean isTemporaryGroup() {
		return true;
	}

	@Override
	public int sizeForProvisioning() throws PEException {
		return groupClass.getCount();
	}

	public boolean isAggregationGroup() {
		return scale == GroupScale.AGGREGATE;
	}

	@Override
	public String toString() {
		StringBuffer display = new StringBuffer(name).append("(");
		if (groupClass == null)
			display.append("cost based");
		else
			display = groupClass.toString(display);
		return display.append(")@").append(dynamicGroupId).toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dynamicGroupId;
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
		DynamicGroup other = (DynamicGroup) obj;
		if (dynamicGroupId != other.dynamicGroupId)
			return false;
		return true;
	}

	@Override
	public int getId() {
		// must return 0 in order for rand dist model to work right
		return 0;
	}
}
