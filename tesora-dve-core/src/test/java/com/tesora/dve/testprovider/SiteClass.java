// OS_STATUS: public
package com.tesora.dve.testprovider;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;

@XmlAccessorType(XmlAccessType.NONE)
public class SiteClass {
	
	@XmlAttribute(name="name")
	String name;

	@XmlElement(name="Site")
	LinkedList<SiteInfo> allSites; // NOPMD by doug on 04/12/12 1:34 PM
	
	@XmlAttribute(name="maxQueries")
	int maxQueriesPerSite;

	Map<String, SiteInfo> allocatedSites = new HashMap<String, SiteInfo>();
	
	public SiteClass() {
	}

	public SiteClass(String name, int maxQueriesPerSite) {
		this.name = name;
		this.maxQueriesPerSite = maxQueriesPerSite;
	}

	public String getName() {
		return name;
	}

	public Collection<PersistentSite> getSites(int count) throws PEException {
		ArrayList<PersistentSite> sites = new ArrayList<PersistentSite>();
		ArrayList<SiteInfo> existingSites = new ArrayList<SiteInfo>(allocatedSites.values());
		Collections.sort(existingSites, SiteInfo.LOAD_ORDER);
		
		try {
			for (SiteInfo siteInfo : existingSites) {
				if (siteInfo.hasCapacity(maxQueriesPerSite)) {
					sites.add(siteInfo.getStorageSite());
					siteInfo.incrementUsageCount();
				}
				if (sites.size() == count)
					break;
			}
			while (sites.size() < count && false == allSites.isEmpty()) {
				SiteInfo siteInfo = allSites.pop();
				PersistentSite storageSite = siteInfo.getStorageSite();
				allocatedSites.put(storageSite.getName(), siteInfo);
				sites.add(storageSite);
				siteInfo.incrementUsageCount();
			}
		} finally {
			if (sites.size() < count)
				returnSites(sites);
		}
		
		return sites;
	}

	public void returnSites(Collection<? extends StorageSite> site2) {
		for (StorageSite site : site2) {
			SiteInfo siteInfo = allocatedSites.get(site.getName());
			if (siteInfo.decrementUsageCount() == 0) {
				allocatedSites.remove(site.getName());
				allSites.push(siteInfo);
			}
		}
	}

}
