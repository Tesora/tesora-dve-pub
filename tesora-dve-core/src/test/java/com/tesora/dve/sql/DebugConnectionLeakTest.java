// OS_STATUS: public
package com.tesora.dve.sql;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

// use this test to debug failures that happen on the maven build but not in eclipse -
// figure out the minimal set that causes the test to fail in eclipse.

@Ignore
@RunWith(AllTests.class)
public class DebugConnectionLeakTest {

	public static TestSuite suite() {
		Class<?>[] testClasses = new Class<?>[] {
				// com.tesora.dve.common.catalog.CatalogTest.class,
				// com.tesora.dve.common.CatalogHelperTest.class,
				// com.tesora.dve.common.resultset.ResultChunkMgrTest.class,
				// com.tesora.dve.common.UrlBalancerTest.class,
				// com.tesora.dve.db.DBNativeTest.class,
				// com.tesora.dve.db.mysql.DBTypeBasedUtilsTest.class,
				// com.tesora.dve.db.mysql.libmy.MyNullBitmapTest.class,
				// com.tesora.dve.db.mysql.libmy.MyParameterTest.class,
				// com.tesora.dve.db.mysql.MysqlNativeTest.class,
				// com.tesora.dve.db.mysql.portal.ComplexPreparedStatementTest.class,
				// com.tesora.dve.db.mysql.portal.MSPDecoderTest.class,
				// com.tesora.dve.db.mysql.portal.MSPPreparedStmtTest.class,
				// com.tesora.dve.distribution.GenerationKeyRangeTest.class,
				com.tesora.dve.groupmanager.LocalTopicTest.class,
				com.tesora.dve.dbc.DBCTest.class,
				com.tesora.dve.queryplan.QueryStepBasicTest.class,
				com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProviderTest.class,
				com.tesora.dve.sql.AggTest.class,
				com.tesora.dve.sql.AlterDistributionTest.class,
				com.tesora.dve.sql.AlterTest.class,
				com.tesora.dve.sql.BigInsertTest.class,
				com.tesora.dve.sql.BlobTest.class,
				com.tesora.dve.sql.BugsMirrorTest.class,
				com.tesora.dve.sql.CatalogQueryTest.class,
				com.tesora.dve.sql.ColumnAliasTest.class,
				com.tesora.dve.sql.CorrelatedSubqueryTest.class,
				com.tesora.dve.sql.CurrentTimestampDefaultValueTest.class,
				com.tesora.dve.sql.DebugTest.class,
				com.tesora.dve.sql.DeleteOrderByLimitTest.class,
				com.tesora.dve.sql.DropTest.class,
				com.tesora.dve.sql.ExplainTest.class,
				com.tesora.dve.sql.FunctionsTest.class,
				com.tesora.dve.sql.GenerationSitesTest.class
		};
		TestSuite suite = new TestSuite();
		for(Class<?> c : testClasses)
			suite.addTest(new JUnit4TestAdapter(c));
		return suite;		
	}
	
}
