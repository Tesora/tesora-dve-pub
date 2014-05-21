// OS_STATUS: public
package com.tesora.dve.sql;

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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import com.tesora.dve.distribution.GenerationKeyRangeTest;
import com.tesora.dve.queryplan.QueryStepBasicTest;
import com.tesora.dve.test.bootstrap.BootstrapTest;
import com.tesora.dve.test.distribution.DistributionTest;
import com.tesora.dve.test.simplequery.SimpleQueryTest;
import com.tesora.dve.test.tuples.TuplesTest;

// use this test to debug failures that happen on the maven build but not in eclipse -
// figure out the minimal set that causes the test to fail in eclipse.

@Ignore
@RunWith(AllTests.class)
public class DebugTest {

	public static TestSuite suite() {
		Class<?>[] testClasses = new Class<?>[] {
				QueryStepBasicTest.class,
				GenerationKeyRangeTest.class,
//				ContainerSqlTest.class,
				BootstrapTest.class,
				TuplesTest.class,
				DistributionTest.class,
				SimpleQueryTest.class,
				OrderByLimitTest.class,
				ScopingTest.class
		};
		TestSuite suite = new TestSuite();
		for(Class<?> c : testClasses)
			suite.addTest(new JUnit4TestAdapter(c));
		return suite;		
	}
	
}
