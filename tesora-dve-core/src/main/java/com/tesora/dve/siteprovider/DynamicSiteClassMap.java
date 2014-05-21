// OS_STATUS: public
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.common.catalog.CatalogEntity;

public class DynamicSiteClassMap {

	private Map<String, DynamicSiteClass> siteClassMap = new ConcurrentHashMap<String, DynamicSiteClass>();

	public DynamicSiteClassMap() {
	}

	public DynamicSiteClass get(String className) {
		return siteClassMap.get(className);
	}

	public void put(DynamicSiteClass dsc) {
		siteClassMap.put(dsc.getName(), dsc);
	}

	public boolean containsKey(String className) {
		return siteClassMap.containsKey(className);
	}

	public Collection<DynamicSiteClass> getAll() {
		return siteClassMap.values();
	}

	public void clear() {
		for (DynamicSiteClass siteClass : siteClassMap.values()) {
			siteClass.clear();
		}
		siteClassMap.clear();
	}

	public List<CatalogEntity> toCatalogEntity(String providerName) {
		List<CatalogEntity> ret = new ArrayList<CatalogEntity>();

		for (DynamicSiteClass dsc : siteClassMap.values()) {
			ret.addAll(dsc.toCatalogEntity(providerName));
		}
		return ret;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (DynamicSiteClass dsc : siteClassMap.values()) {
			builder.append(dsc.toString() + "\n");
		}
		return builder.toString();
	}
}
