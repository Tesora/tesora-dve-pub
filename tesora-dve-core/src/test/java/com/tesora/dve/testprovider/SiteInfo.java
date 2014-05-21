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

import java.util.Comparator;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.exceptions.PEException;

@XmlAccessorType(XmlAccessType.NONE)
public class SiteInfo {
	
	static final Comparator<SiteInfo> LOAD_ORDER = new Comparator<SiteInfo>() {
		@Override
		public int compare(SiteInfo o1, SiteInfo o2) {
			return o1.allocatedCount - o2.allocatedCount;
		}
	};

	@XmlAttribute
	String name;
	
	@XmlAttribute
	String url;

	@XmlAttribute
	String user;

	@XmlAttribute
	String password;
	
	int allocatedCount = 0;

	private PersistentSite site = null;
	
	public SiteInfo() {
	}

	public SiteInfo(String name, String url) {
		this.name = name;
		this.url = url;
	}

	public String getName() {
		return name;
	}

	public boolean hasCapacity(int maxQueriesPerSite) {
		return allocatedCount < maxQueriesPerSite;
	}

	public void incrementUsageCount() {
		++allocatedCount;
	}
	
	public int decrementUsageCount() {
		return --allocatedCount;
	}

	public PersistentSite getStorageSite() throws PEException {
		if (site == null) {
			site  = new PersistentSite(name, new SiteInstance(name, url, user, password));
		}
		return site;
	}
}
