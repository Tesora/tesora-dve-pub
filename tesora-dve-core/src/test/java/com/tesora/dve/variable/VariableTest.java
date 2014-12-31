package com.tesora.dve.variable;

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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import com.tesora.dve.variables.VariableService;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PEMappedException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryStepGetGlobalVariableOperation;
import com.tesora.dve.queryplan.QueryStepGetSessionVariableOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.SSConnectionAccessor;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.test.simplequery.SimpleQueryTest;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.VariableManager;
import com.tesora.dve.worker.MysqlTextResultChunkProvider;
import com.tesora.dve.worker.UserCredentials;

public class VariableTest extends PETest {

	static final String PERSISTENT_GROUP_VARIABLE = "persistent_group";

	@SuppressWarnings("hiding")
	public Logger logger = Logger.getLogger(VariableTest.class);

	SSConnectionProxy conProxy;
	SSConnection ssConnection;

	UserDatabase db;
	PersistentGroup sg;

	QueryPlan plan;

	@BeforeClass
	public static void setup() throws Throwable {
		TestCatalogHelper.createTestCatalog(PETest.class);
		bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		SimpleQueryTest.declareSchema(pcr);
		pcr.disconnect();
	}

	public VariableTest() {
	}

	@Before
	public void setupUserData() throws PEException, SQLException {
        populateSites(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());
		conProxy = new SSConnectionProxy();
		ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
		SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
		ssConnection.startConnection(new UserCredentials(bootHost.getProperties()));
		ssConnection.setPersistentDatabase(catalogDAO.findDatabase("TestDB"));
		db = ssConnection.getPersistentDatabase();
		sg = db.getDefaultStorageGroup();
		plan = new QueryPlan();
	}

	@After
	public void testCleanup() throws PEException {
		plan = null;
		if(conProxy != null)
			conProxy.close();
		conProxy = null;
	}

	@Test
	public void globalVariableTest() throws PEException {
		assertEquals(Boolean.FALSE, KnownVariables.SLOW_QUERY_LOG.getValue(null));
		KnownVariables.SLOW_QUERY_LOG.setGlobalValue(ssConnection,"yes");
		assertEquals(Boolean.TRUE, KnownVariables.SLOW_QUERY_LOG.getValue(null));
	}

	@Test(expected = PEMappedException.class)
	public void globalVariableNotExistsTest() throws PEException {
		Singletons.require(VariableService.class).getVariableManager().lookupMustExist(null,"no-such-variable");
	}

	@Test
	public void getVersionCommentTest() throws PEException {
        assertEquals(Singletons.require(HostService.class).getDveServerVersionComment(),
        		KnownVariables.VERSION_COMMENT.getGlobalValue(null));
	}

	@Test
	public void getVersionTest() throws PEException {
        assertEquals(Singletons.require(HostService.class).getDveServerVersion(),
        		KnownVariables.VERSION.getGlobalValue(null));
	}

	@Test(expected = PEException.class)
	public void setVersionCommentTest() throws PEException {
		KnownVariables.VERSION_COMMENT.setGlobalValue(ssConnection,"hello");
	}

	@Test
	public void globalVariableQSOTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.GLOBAL), "sql_logging", "yes"),
				results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetGlobalVariableOperation(KnownVariables.SQL_LOGGING), results);
		assertTrue(results.hasResults());
		assertEquals("YES", results.getSingleColumnValue(1, 1));
	}

	@Test
	public void sessionVariableQSOTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION), "character_set_client",
				"latin1"), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.CHARACTER_SET_CLIENT), results);
		assertTrue(results.hasResults());
		assertEquals("latin1", results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION), "character_set_client",
				"utf8"), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.CHARACTER_SET_CLIENT), results);
		assertTrue(results.hasResults());
		assertEquals("utf8", results.getSingleColumnValue(1, 1));
	}

	@Test(expected = PEMappedException.class)
	public void sessionVariableNotExistsTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION), "invalid-session-name",
				"value1"), results);
		assertFalse(results.hasResults());
	}

	@Test
	public void setPolicyTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION), "dynamic_policy",
				"OnPremisePolicy"), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.DYNAMIC_POLICY), results);
		assertTrue(results.hasResults());
		assertEquals("OnPremisePolicy", results.getSingleColumnValue(1, 1));
	}

	@Test(expected = PENotFoundException.class)
	public void setPolicyFailTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION), "dynamic_policy",
				"value1"), results);
		assertFalse(results.hasResults());
	}

	@Test
	public void setStorageGroupTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION),
				PERSISTENT_GROUP_VARIABLE, PEConstants.DEFAULT_GROUP_NAME), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.PERSISTENT_GROUP), results);
		assertTrue(results.hasResults());
		assertEquals(PEConstants.DEFAULT_GROUP_NAME, results.getSingleColumnValue(1, 1));
	}

	@Test(expected = PENotFoundException.class)
	public void setStorageGroupFailTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION),
				PERSISTENT_GROUP_VARIABLE, "value1"), results);
		assertFalse(results.hasResults());
	}

	@Test
	public void getVariableUpperCaseTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.PERSISTENT_GROUP), results);
		assertTrue(results.hasResults());
		assertEquals(1, results.getNumRowsAffected());
	}

	@Test
	public void clientCharSet() throws Throwable {
		String origCharSet =
				KnownVariables.CHARACTER_SET_CLIENT.getSessionValue(ssConnection).getName();
		String newCharset = "utf8";
		if (!origCharSet.toLowerCase().equals("utf8"))
			newCharset = "latin1";

		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(new VariableScope(VariableScopeKind.SESSION),
				VariableConstants.CHARACTER_SET_CLIENT_NAME, newCharset), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.CHARACTER_SET_CLIENT), results);
		assertTrue(results.hasResults());
		assertEquals(newCharset, results.getSingleColumnValue(1, 1));
	}

	@Test
	public void setValidCollation() throws Throwable {
		String origCollation = 
				KnownVariables.COLLATION_CONNECTION.getSessionValue(ssConnection);

		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.COLLATION_CONNECTION), results);
		assertTrue(results.hasResults());
		assertEquals(origCollation, results.getSingleColumnValue(1, 1));

		String utfCollation = "utf8_unicode_ci";
		ssConnection.setSessionVariable(VariableConstants.COLLATION_CONNECTION_NAME, utfCollation);
		assertEquals(utfCollation,
				KnownVariables.COLLATION_CONNECTION.getSessionValue(ssConnection));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.COLLATION_CONNECTION),
				results);
		assertTrue(results.hasResults());
		assertEquals(utfCollation, results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSelectAllOperation(sg,ssConnection, db, BroadcastDistributionModel.SINGLETON,
				"select @@session.collation_connection"), results);
		assertTrue(results.hasResults());
		assertEquals(utfCollation, results.getSingleColumnValue(1, 1));

		ssConnection.setSessionVariable(VariableConstants.COLLATION_CONNECTION_NAME, origCollation);
		assertEquals(origCollation, 
				KnownVariables.COLLATION_CONNECTION.getSessionValue(ssConnection));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(KnownVariables.COLLATION_CONNECTION), results);
		assertTrue(results.hasResults());
		assertEquals(origCollation, results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSelectAllOperation(sg,ssConnection, db, BroadcastDistributionModel.SINGLETON,
				"select @@session.collation_connection"), results);
		assertTrue(results.hasResults());
		assertEquals(origCollation, results.getSingleColumnValue(1, 1));
	}

	@Test
	public void setInvalidCollation() throws Throwable {
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				KnownVariables.COLLATION_CONNECTION.setSessionValue(ssConnection, "latin1_junk_ci");
			}
		}.assertError(SchemaException.class, MySQLErrors.unknownCollationFormatter, "latin1_junk_ci");
	}

	/** PE-1154 */
	@Test
	public void setLongQueryTime() throws Throwable {
		assertEquals(new Double(10.0), KnownVariables.LONG_QUERY_TIME.getGlobalValue(ssConnection));
		KnownVariables.LONG_QUERY_TIME.setGlobalValue(ssConnection,"25.5");
		assertEquals(new Double(25.5), KnownVariables.LONG_QUERY_TIME.getGlobalValue(ssConnection));
	}

	/** PE-1156 */
	@SuppressWarnings("unchecked")
	@Test
	public void setGroupConcatMaxLen() throws Throwable {
		VariableManager vm = Singletons.require(VariableService.class).getVariableManager();
		VariableHandler<Long> var = (VariableHandler<Long>) vm.lookupMustExist(null,"group_concat_max_len"); 
		assertEquals(new Long(1024), var.getSessionValue(ssConnection));
		var.setSessionValue(ssConnection, "5");
		assertEquals(new Long(5), var.getSessionValue(ssConnection)); 
	}

	/** PE-1128 */
	@Test
	public void setAutocommit() throws Throwable {
		assertEquals(Boolean.TRUE, KnownVariables.AUTOCOMMIT.getSessionValue(ssConnection));
		KnownVariables.AUTOCOMMIT.setSessionValue(ssConnection, "0");
		assertEquals(Boolean.FALSE, KnownVariables.AUTOCOMMIT.getSessionValue(ssConnection));
		KnownVariables.AUTOCOMMIT.setSessionValue(ssConnection, "ON");
		assertEquals(Boolean.TRUE, KnownVariables.AUTOCOMMIT.getSessionValue(ssConnection));
		KnownVariables.AUTOCOMMIT.setSessionValue(ssConnection, "OFF");
		assertEquals(Boolean.FALSE, KnownVariables.AUTOCOMMIT.getSessionValue(ssConnection));
				
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				KnownVariables.AUTOCOMMIT.setSessionValue(ssConnection, null);
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongValueForVariable, "autocommit", "NULL");

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				KnownVariables.AUTOCOMMIT.setSessionValue(ssConnection, "");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongValueForVariable, "autocommit", "");

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				KnownVariables.AUTOCOMMIT.setSessionValue(ssConnection, "2");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongValueForVariable, "autocommit", "2");
	}

	@Test
	public void setTimeZone() throws Throwable {
		// make sure the default values are properly set
		// now that we support scopes - we should toss over default_time_zone in favor of time_zone with a global scope
		
		String peTimeZoneDefault = KnownVariables.TIME_ZONE.getGlobalValue(ssConnection);		
		assertEquals("+00:00", peTimeZoneDefault);
		assertEquals("+00:00", KnownVariables.TIME_ZONE.getSessionValue(ssConnection)); 

		// change the session variable
		KnownVariables.TIME_ZONE.setSessionValue(ssConnection, "+05:00");
		assertEquals("+05:00", KnownVariables.TIME_ZONE.getSessionValue(ssConnection));

		// change the default PE time zone
		KnownVariables.TIME_ZONE.setGlobalValue(ssConnection,"-09:00");
		// new connection should have new default
		SSConnectionProxy conProxy = new SSConnectionProxy();
		try {
			SSConnection ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
			SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
			ssConnection.startConnection(new UserCredentials(bootHost.getProperties()));
			assertEquals("-09:00",  KnownVariables.TIME_ZONE.getSessionValue(ssConnection));
		} finally {
			conProxy.close();
		}
	}

	/*
	@Test
	public void getLiteralSessionVariableTest() throws PEException {
		throw new PEException("fill me in");
//        assertEquals("YES", Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo("have_innodb").getHandler()
//				.getValue(ssConnection, "have_innodb"));
	}

	@Test(expected = PEException.class)
	public void setLiteralSessionVariableFailTest() throws Throwable {
		throw new Throwable ("fill me in");
//        Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo("have_innodb").getHandler()
//				.setValue(ssConnection, "have_innodb", "NO");
	}
*/

	@Test
	public void setGroupServiceVariableTest() throws PEException {
//        HostService hostService = Singletons.require(HostService.class);
		KnownVariables.GROUP_SERVICE.setPersistentValue(ssConnection, "HaZeLCaST");
		assertEquals("HaZeLCaST",
				KnownVariables.GROUP_SERVICE.getValue(null));
		KnownVariables.GROUP_SERVICE.setPersistentValue(ssConnection, "Localhost");
		assertEquals("Localhost", 
				KnownVariables.GROUP_SERVICE.getValue(null));
	}

	@Test(expected = PEException.class)
	public void setGroupServiceVariableFailTest() throws PEException {
		KnownVariables.GROUP_SERVICE.setPersistentValue(ssConnection, "InvalidValue");

	}

	private void executeQuery(QueryStepOperation qso, DBResultConsumer results) throws Throwable {
		QueryPlan qp = new QueryPlan();
		qp.addStep(qso);
		qp.executeStep(ssConnection, results);
	}

}
