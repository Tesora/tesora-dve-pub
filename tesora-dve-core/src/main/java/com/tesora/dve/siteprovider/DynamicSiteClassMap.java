// OS_STATUS: public
package com.tesora.dve.siteprovider;

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
