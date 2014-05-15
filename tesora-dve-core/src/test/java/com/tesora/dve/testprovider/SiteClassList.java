// OS_STATUS: public
package com.tesora.dve.testprovider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

import com.tesora.dve.testprovider.SiteClass;

public class SiteClassList {
	public SiteClassList(Collection<SiteClass> values) {
		siteClassList = new ArrayList<SiteClass>(values);
	}

	public SiteClassList() {
	}

	@XmlElement(name="SiteClass")
	List<SiteClass> siteClassList;
}