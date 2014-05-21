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

import static org.junit.Assert.*;

import org.junit.Test;

import com.tesora.dve.server.statistics.ServerStatistics;
import com.tesora.dve.server.statistics.SiteStatKey;
import com.tesora.dve.server.statistics.TimingSet;
import com.tesora.dve.server.statistics.TimingValue;

public class ServerStatisticsTest {
	// This is the order that should be returned by getAllSiteTimingsOrdered()
	// The data for this test is in SiteStatKeyTest
	// The -1 entries are so the duplicates don't get loaded into the list
	static final int expectedOrder[] = { 2, 3, 0 , 5, -1, 4, 1, -1, -1 };
	
	// This test validates that the TimingSets are returned in the proper order
	// from getAllSiteTimingsOrdered()
	@Test
	public void orderedTimingsTest() {
		ServerStatistics svrStats = new ServerStatistics();
		
		int index = 0;
		for ( SiteStatKey ssk : SiteStatKeyTest.testData ) {
			if ( expectedOrder[index] >= 0 )
				svrStats.addSiteTiming(new TimingSet(ssk, new TimingValue(), new TimingValue(), new TimingValue(), new TimingValue()));
			index++;
		}

		int currentPos = 0;
		for ( TimingSet ts : svrStats.getAllSiteTimingsOrdered() ) {
//			System.out.println("cp="+currentPos+" ssk="+ts.getStatKey().toString()+
//					" order="+getPosition(ts.getStatKey()));
			assertEquals("CurrentPos="+currentPos,currentPos,getPosition(ts.getStatKey()));
			currentPos++;
		}
	}

	private int getPosition(SiteStatKey ssk) {
		for ( int i = 0; i < SiteStatKeyTest.testData.length; i++ ) {
			if ( SiteStatKeyTest.testData[i].equals(ssk) )
				return expectedOrder[i];
		}
		
		return -1;
	}
}
