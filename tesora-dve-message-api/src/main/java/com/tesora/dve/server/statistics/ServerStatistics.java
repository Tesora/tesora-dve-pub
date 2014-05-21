package com.tesora.dve.server.statistics;

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
