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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;

@XmlRootElement(name="StaticSiteProvider")
@XmlAccessorType(XmlAccessType.NONE)
public class SiteProviderConfig {
	
	@XmlElement(name="SiteClassList")
	@XmlJavaTypeAdapter(SiteClassMapAdapter.class)
	Map<String, SiteClass> siteClassMap = new HashMap<String, SiteClass>();

	public void addSiteClass(SiteClass siteClass) {
		siteClassMap.put(siteClass.getName(), siteClass);
	}

	public Collection<PersistentSite> getSitesByClass(String siteClass, int count) throws PEException {
		Collection<PersistentSite> sites = null;
		if (siteClassMap.containsKey(siteClass)) {
			sites = siteClassMap.get(siteClass).getSites(count);
		} else
			throw new PEException("Invalid SiteClass " + siteClass);
		return sites;
	}

	public void returnSitesByClass(String siteClass, Collection<? extends StorageSite> site) {
		siteClassMap.get(siteClass).returnSites(site);
	}
}
