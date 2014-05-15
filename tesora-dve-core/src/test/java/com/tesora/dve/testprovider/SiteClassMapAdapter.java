// OS_STATUS: public
package com.tesora.dve.testprovider;

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