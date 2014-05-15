// OS_STATUS: public
package com.tesora.dve.siteprovider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.StorageSite;

public class DynamicSiteClass {

	public static final Logger log = Logger.getLogger(DynamicSiteClass.class);

	private String name;
	private String alternate;
	
	private Map<String, DynamicSiteInfo> sitesMap = new LinkedHashMap<String, DynamicSiteInfo>();

	public DynamicSiteClass() {
	}

	public DynamicSiteClass(String name) {
		this.name = name;
	}

	public DynamicSiteClass(String name, String alternate) {
		this.name = name;
		this.alternate = alternate;
	}

	public DynamicSiteClass(DynamicSiteClass cls) {
		this.name = cls.name;
		this.alternate = cls.alternate;
		this.sitesMap = new LinkedHashMap<String, DynamicSiteInfo>(cls.sitesMap);
	}

	public void clear() {
		for (DynamicSiteInfo siteInfo : sitesMap.values()) {
			if (siteInfo.getCurrentQueries() > 0) {
				log.warn("Site " + siteInfo.getFullName() + " has " + siteInfo.getCurrentQueries() + " queries pending");
			}
		}
		sitesMap.clear();
	}

	public String getName() {
		return name;
	}
	
	public String getAlternate() {
		return alternate;
	}
	
	public void setAlternate(String alternate) {
		this.alternate = alternate;
	}

	public Collection<DynamicSiteInfo> getSites() {
		return sitesMap.values();
	}

	public DynamicSiteInfo getSite(String fullName) {
		return sitesMap.get(fullName);
	}

	public DynamicSiteInfo getSiteByShortName(String shortName) {
		for (DynamicSiteInfo siteInfo : sitesMap.values()) {
			if (siteInfo.getShortName().equals(shortName))
				return siteInfo;
		}
		return null;
	}

	public void addSite(DynamicSiteInfo site) {
		sitesMap.put(site.getFullName(), site);
	}

	public void replaceSite(DynamicSiteInfo site) {
		sitesMap.put(site.getFullName(), site);
	}

	public void removeSite(DynamicSiteInfo site) {
		sitesMap.remove(site.getFullName());
	}

	public Collection<? extends StorageSite> allocateSites(int count) {

		List<DynamicSiteInfo> result = new ArrayList<DynamicSiteInfo>();
		List<DynamicSiteInfo> sortedSites = new ArrayList<DynamicSiteInfo>(sitesMap.values());

		Collections.sort(sortedSites, DynamicSiteInfo.LOAD_AND_TIME_ORDER);

		for (DynamicSiteInfo info : sortedSites) {
			if (info.isAvailable() && info.hasCapacity()) {
				result.add(info);
				info.incrementUsageCount();
			}
			if (result.size() == count)
				break;
		}
		return result;
	}

	public List<StorageSite> returnSites(Collection<? extends StorageSite> sites, boolean unused) {

		List<StorageSite> remaining = new ArrayList<StorageSite>();

		for (StorageSite site : sites) {
			DynamicSiteInfo info = sitesMap.get(site.getName());
			if (info != null) {
				info.decrementUsageCount(unused);

				// If any sites are ready for removal then do it now
				if (info.isReadyForRemoval())
					sitesMap.remove(info);
			} else {
				remaining.add(site);
			}
		}
		return remaining;
	}

	public void cleanupAndRemove() {
		for (Iterator<DynamicSiteInfo> iter = sitesMap.values().iterator(); iter.hasNext();) {
			DynamicSiteInfo info = iter.next();

			if (info.isReadyForRemoval())
				iter.remove();
		}
	}

	public void markForRemoval() {
		for (DynamicSiteInfo info : sitesMap.values()) {
			info.markForRemoval();
		}
	}

	public List<CatalogEntity> toCatalogEntity(String providerName) {
		List<CatalogEntity> ret = new ArrayList<CatalogEntity>();

		for (DynamicSiteInfo site : getSites()) {
			ret.add(site.toCatalogEntity(providerName, name));
		}
		return ret;
	}

	protected String toStringThis() {
		return "DynamicSiteClass name=" + name;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append(toStringThis() + "\n");
		builder.append("sites=" + getSites().size() + "\n");
		for (DynamicSiteInfo site : getSites()) {
			builder.append("  " + site.toString() + "\n");
		}
		return builder.toString();
	}
}
