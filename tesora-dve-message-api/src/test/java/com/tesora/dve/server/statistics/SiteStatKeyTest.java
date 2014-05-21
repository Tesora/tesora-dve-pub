// OS_STATUS: public
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

import com.tesora.dve.server.statistics.SiteStatKey;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.server.statistics.SiteStatKey.SiteType;

public class SiteStatKeyTest {

	static final SiteStatKey testData[] = {
			new SiteStatKey(SiteType.DYNAMIC, "dyn_site1", OperationClass.DDL),
			new SiteStatKey(SiteType.DYNAMIC, "dyn_site1", OperationClass.EXECUTE),
			new SiteStatKey(SiteType.GLOBAL, null, OperationClass.QUERY),
			new SiteStatKey(SiteType.PERSISTENT, "pers_site2", OperationClass.EXECUTE),
			new SiteStatKey(SiteType.PERSISTENT, "pers_site2", OperationClass.EXECUTE),
			new SiteStatKey(SiteType.PERSISTENT, "pers_site1", OperationClass.FETCH),
			new SiteStatKey(SiteType.GLOBAL, null, OperationClass.UPDATE),
			new SiteStatKey(SiteType.GLOBAL, null, OperationClass.UPDATE),
			new SiteStatKey(SiteType.DYNAMIC, "dyn_site1", OperationClass.DDL)
	};
	static final int expectedValues[] = { -1, 1, -2, 0, 1, 2, 0, -1 };

	// This test validates that the custom compareTo method is functioning properly
	@Test
	public void compareToTest() {

		for ( int i = 0; i < testData.length-1; i++ ) {
			assertEquals("Loop index is " + i, expectedValues[i], testData[i].compareTo(testData[i+1]));
		}
	}

}
