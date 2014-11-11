package com.tesora.dve.sql.transform.execution;

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

import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;

public class RebuiltPlan {
	protected RootExecutionPlan ep;
	protected boolean clearCache = true;
	protected final SchemaCacheKey<?>[] cacheKeys;
	protected final LockType lockType;
	protected final ConnectionValuesMap boundValues;
	
	public RebuiltPlan(RootExecutionPlan ep, ConnectionValuesMap boundValues, boolean clearCache, SchemaCacheKey<?>[] keys, LockType lt) {
		this.ep = ep;
		this.clearCache = clearCache;
		this.cacheKeys = keys;
		this.lockType = lt;
		this.boundValues = boundValues;
	}

	public RootExecutionPlan getEp() {
		return ep;
	}

	public boolean isClearCache() {
		return clearCache;
	}
	
	public SchemaCacheKey<?>[] getCacheKeys() {
		return cacheKeys;
	}
	
	public LockType getLockType() {
		return lockType;
	}
	
	public ConnectionValuesMap getBoundValues() {
		return boundValues;
	}
}
