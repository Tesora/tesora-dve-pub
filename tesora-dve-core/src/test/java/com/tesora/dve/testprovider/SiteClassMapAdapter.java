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

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import com.tesora.dve.testprovider.SiteClass;

public class SiteClassMapAdapter extends XmlAdapter<SiteClassList, Map<String, SiteClass>> {
	
	@Override
	public Map<String, SiteClass> unmarshal(SiteClassList v)
			throws Exception {
		Map<String, SiteClass> newMap = new HashMap<String, SiteClass>();
		for (SiteClass siteClass : v.siteClassList) {
			newMap.put(siteClass.name, siteClass);
		}
		return newMap;
	}

	@Override
	public SiteClassList marshal(Map<String, SiteClass> v)
			throws Exception {
		return new SiteClassList(v.values());
	}
	
}