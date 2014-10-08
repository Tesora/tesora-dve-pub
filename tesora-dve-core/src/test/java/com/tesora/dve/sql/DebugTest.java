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

import com.tesora.dve.common.CatalogHelperTest;
import com.tesora.dve.common.UrlBalancerTest;
import com.tesora.dve.common.catalog.CatalogTest;
import com.tesora.dve.groupmanager.LocalTopicTest;
import com.tesora.dve.queryplan.QueryStepBasicTest;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProviderTest;
import com.tesora.dve.sql.schema.DBEnumTypeTest;
import com.tesora.dve.sql.schema.TestRoundTrip;
import com.tesora.dve.sql.scripted.NonMirrorBugsTest;
import com.tesora.dve.sql.showservers.ShowServersTest;
import com.tesora.dve.sql.statement.InsertNormalizationTest;
import com.tesora.dve.sql.statement.JoinGraphTest;
import com.tesora.dve.sql.statement.RangeDistTypeEquivalencyTest;
import com.tesora.dve.sql.statement.ResolvingTest;
import com.tesora.dve.sql.statement.SelectNormalizationTest;
import com.tesora.dve.test.autoincrement.AutoIncrementTrackerTest;
import com.tesora.dve.test.bootstrap.BootstrapTest;
import com.tesora.dve.test.container.ContainerFunctionalTest;
import com.tesora.dve.test.container.ContainerSqlTest;
import com.tesora.dve.test.distribution.DistributionTest;
import com.tesora.dve.test.externalservice.ExternalServiceTest;
import com.tesora.dve.test.security.SiteSecurityTest;
import com.tesora.dve.test.simplequery.SimpleQueryTest;
import com.tesora.dve.test.tuples.TuplesTest;
import com.tesora.dve.worker.UserAuthenticationTest;

// use this test to debug failures that happen on the maven build but not in eclipse -
// figure out the minimal set that causes the test to fail in eclipse.

@Ignore
@RunWith(AllTests.class)
public class DebugTest {

	public static TestSuite suite() {
		Class<?>[] testClasses = new Class<?>[] {
				SiteSecurityTest.class,
				BootstrapTest.class,
				AutoIncrementTrackerTest.class,
				SimpleQueryTest.class,
				ExternalServiceTest.class,
				ContainerFunctionalTest.class,
				ContainerSqlTest.class,
				TuplesTest.class,
				DistributionTest.class,
				UserAuthenticationTest.class,
				LocalTopicTest.class,
				OnPremiseSiteProviderTest.class,
				QueryStepBasicTest.class,
				CatalogTest.class,
				UrlBalancerTest.class,
				CatalogHelperTest.class,
				ShowTest.class,
				ReplaceIntoTest.class,
				UnionTest.class,
				GroupProviderDDLTest.class,
				InsertMysqlConnTest.class,
				DebugConnectionLeakTest.class,
				SelectTest.class,
				JoinTest.class,
				XATest.class,
				ViewDDLTest.class,
				RawPlanTest.class,
				FunctionsTest.class,
				BigInsertTest.class,
				NonMirrorBugsTest.class,
				CreateTableAsSelectTest.class,
				SimpleContainerTest.class,
				TableMaintenanceTest.class,
				IgnoreForeignKeyTest.class,
				TextPrepareTest.class,
				ExplainTest.class,
				ShowServersTest.class,
				SchemaSystemTest.class,
				KillTest.class,
				PrepStmtTest.class,
				UseAffectedRowsTest.class,
				DeleteOrderByLimitTest.class,
				UserTest.class,
				DBEnumTypeTest.class,
				TestRoundTrip.class,
				MultitenantForeignKeyTest.class,
				BalancedPersistentGroupTest.class,
				TruncateTest.class,
				CurrentTimestampDefaultValueTest.class,
				GenerationSitesTest.class,
				ScopingTest.class,
				InsertIntoSelectTest.class,
				TestEmptyCatalog.class,
				SimpleMultitenantTest.class,
				UpdateTest.class,
				ResolvingTest.class,
				RangeDistTypeEquivalencyTest.class,
				InsertNormalizationTest.class,
				SelectNormalizationTest.class,
				JoinGraphTest.class,
				SQLVariableTest.class,
				InsertAutoincrementTest.class,
				TestCreates.class,
				NullDataTest.class,
				ViewTest.class,
				AggBugsTest.class,
				LargeMaxPktTest.class,
				MetadataInjectionTest.class,
				AlterTest.class,
				OrderByLimitTest.class,
				AggTest.class,
				ColumnAliasTest.class,
				
				
		};
		TestSuite suite = new TestSuite();
		for(Class<?> c : testClasses)
			suite.addTest(new JUnit4TestAdapter(c));
		return suite;		
	}
	
}
