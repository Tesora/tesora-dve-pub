// OS_STATUS: public
package com.tesora.dve.server.statistics;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class ServerStatistics implements Serializable {
	
	private static final long serialVersionUID = 1L;

	TimingSet globalQuery;
	TimingSet globalUpdate;
	
	List<TimingSet> siteTimings = new LinkedList<TimingSet>();

	public ServerStatistics() {
	}
	
	public ServerStatistics(TimingSet globalQuery, TimingSet globalUpdate) {
		super();
		this.globalQuery = globalQuery;
		this.globalUpdate = globalUpdate;
	}
	
	public void addSiteTiming(TimingSet ts) {
		siteTimings.add(ts);
	}
	
	public Collection<TimingSet> getAllSiteTimings() {
		return siteTimings;
	}
	
	public Collection<TimingSet> getAllSiteTimingsOrdered() {
		Collections.sort(siteTimings, new Comparator<TimingSet>() {
			@Override
			public int compare(TimingSet ts1, TimingSet ts2) {
				return ts1.getStatKey().compareTo(ts2.getStatKey());
			}
		});
		return siteTimings;
	}

	public TimingSet getGlobalQuery() {
		return globalQuery;
	}

	public TimingSet getGlobalUpdate() {
		return globalUpdate;
	}

	public void setGlobalQuery(TimingSet globalQuery) {
		this.globalQuery = globalQuery;
	}

	public void setGlobalUpdate(TimingSet globalUpdate) {
		this.globalUpdate = globalUpdate;
	}
}
