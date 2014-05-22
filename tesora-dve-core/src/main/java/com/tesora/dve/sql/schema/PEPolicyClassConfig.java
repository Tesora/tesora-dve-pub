package com.tesora.dve.sql.schema;

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

import com.tesora.dve.common.catalog.DynamicGroupClass;

public class PEPolicyClassConfig {
	
	PolicyClass policyClass;
	String providerName;
	String poolName;
	Integer count;
	DynamicGroupClass dynClass;
	
	// transient constructor
	public PEPolicyClassConfig(PolicyClass pc, String provider, String pool, Integer count) {
		this.policyClass = pc;
		this.providerName = provider;
		this.poolName = pool;
		this.count = count;
		this.dynClass = null;
	}
	
	public PEPolicyClassConfig(PolicyClass pc, DynamicGroupClass p) {
		this.policyClass = pc;
		this.providerName = p.getProvider();
		this.poolName = p.getPoolName();
		this.count = p.getCount();
		this.dynClass = p;
	}
	
	public DynamicGroupClass update(DynamicGroupClass in) {
		DynamicGroupClass p = in;
		if (p == null)
			p = new DynamicGroupClass();
		p.setPoolName(poolName);
		p.setProvider(providerName);
		p.setCount(count);
		return p;
	}

    public void mergeDefaultsFrom(PEPolicyClassConfig other){

        if (other == null)
            return;

        if (this.policyClass == null)
            this.policyClass = other.policyClass;
        if (this.providerName == null)
            this.providerName = other.providerName;
        if (this.poolName == null)
            this.poolName = other.poolName;
        if (this.count == null)
            this.count = other.count;
    }
	
	public PolicyClass getPolicyClass() {
		return policyClass;
	}
	
	// alter support
	public void setProvider(String s) {
		providerName = s;
	}
	
	public String getProvider() {
		return providerName;
	}
	
	public void setPoolName(String s) {
		poolName = s;
	}
	
	public String getPoolName() {
		return poolName;
	}
	
	public void setCount(int v) {
		count = v;
	}
	
	public int getCount() {
		return count;
	}

	public DynamicGroupClass getDynamicClass() {
		return dynClass;
	}

}
