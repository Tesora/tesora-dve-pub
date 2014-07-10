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
import static org.junit.Assert.fail;

import java.sql.SQLException;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.*;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
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
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepGetGlobalVariableOperation;
import com.tesora.dve.queryplan.QueryStepGetSessionVariableOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.test.simplequery.SimpleQueryTest;
import com.tesora.dve.variables.Variables;
import com.tesora.dve.variables.VariableHandler;
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
	public static void setup() throws Exception {
		TestCatalogHelper.createTestCatalog(PETest.class, 2);
		bootHost = BootstrapHost.startServices(PETest.class);
        populateMetadata(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());
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
		if(plan != null)
			plan.close();
		plan = null;
		if(conProxy != null)
			conProxy.close();
		conProxy = null;
	}

	@Test
	public void globalVariableTest() throws PEException {
        HostService hostService = Singletons.require(HostService.class);

		String val = hostService.getGlobalVariable(catalogDAO, "slow_query_log");
		assertEquals("0", val);
		hostService.setGlobalVariable(catalogDAO, "slow_query_log", "yes");
		val = hostService.getGlobalVariable(catalogDAO, "slow_query_log");
		assertEquals("yes", val);
	}

	@Test(expected = PENotFoundException.class)
	public void globalVariableNotExistsTest() throws PEException {
        Singletons.require(HostService.class).getGlobalVariable(catalogDAO, "no-such-variable");
	}

	@Test
	public void getVersionCommentTest() throws PEException {
        assertEquals(Singletons.require(HostService.class).getDveServerVersionComment(),
				Singletons.require(HostService.class).getGlobalVariable(catalogDAO, "version_comment"));
	}

	@Test
	public void getVersionTest() throws PEException {
        assertEquals(Singletons.require(HostService.class).getDveServerVersion(),
				Singletons.require(HostService.class).getGlobalVariable(catalogDAO, "version"));
	}

	@Test(expected = PENotFoundException.class)
	public void setVersionCommentTest() throws PEException {
        Singletons.require(HostService.class).setGlobalVariable(catalogDAO, "version_comment", "hello");
	}

	@Test
	public void globalVariableQSOTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.DVE, null, "sql_logging", "yes"),
				results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetGlobalVariableOperation("sql_logging"), results);
		assertTrue(results.hasResults());
		assertEquals("yes", results.getSingleColumnValue(1, 1));
	}

	@Test
	public void sessionVariableQSOTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null, "character_set_client",
				"latin1"), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.CHARACTER_SET_CLIENT), results);
		assertTrue(results.hasResults());
		assertEquals("latin1", results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null, "character_set_client",
				"utf8"), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.CHARACTER_SET_CLIENT), results);
		assertTrue(results.hasResults());
		assertEquals("utf8", results.getSingleColumnValue(1, 1));
	}

	@Test(expected = PENotFoundException.class)
	public void sessionVariableNotExistsTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null, "invalid-session-name",
				"value1"), results);
		assertFalse(results.hasResults());
	}

	@Test
	public void setPolicyTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null, "dynamic_policy",
				"OnPremisePolicy"), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.DEFAULT_DYNAMIC_POLICY), results);
		assertTrue(results.hasResults());
		assertEquals("OnPremisePolicy", results.getSingleColumnValue(1, 1));
	}

	@Test(expected = PENotFoundException.class)
	public void setPolicyFailTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null, "dynamic_policy",
				"value1"), results);
		assertFalse(results.hasResults());
	}

	@Test
	public void setStorageGroupTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null,
				PERSISTENT_GROUP_VARIABLE, PEConstants.DEFAULT_GROUP_NAME), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.PERSISTENT_GROUP), results);
		assertTrue(results.hasResults());
		assertEquals(PEConstants.DEFAULT_GROUP_NAME, results.getSingleColumnValue(1, 1));
	}

	@Test(expected = PENotFoundException.class)
	public void setStorageGroupFailTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null,
				PERSISTENT_GROUP_VARIABLE, "value1"), results);
		assertFalse(results.hasResults());
	}

	@Test
	public void getVariableUpperCaseTest() throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.PERSISTENT_GROUP), results);
		assertTrue(results.hasResults());
		assertEquals(1, results.getNumRowsAffected());
	}

	@Test
	public void clientCharSet() throws Throwable {
		String origCharSet =
				Variables.CHARACTER_SET_CLIENT.getSessionValue(ssConnection);
		String newCharset = "utf8";
		if (!origCharSet.toLowerCase().equals("utf8"))
			newCharset = "latin1";

		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null,
				ClientCharSetSessionVariableHandler.VARIABLE_NAME, newCharset), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.CHARACTER_SET_CLIENT), results);
		assertTrue(results.hasResults());
		assertEquals(newCharset, results.getSingleColumnValue(1, 1));
	}

	@Test
	public void setValidCollation() throws Throwable {
		String origCollation = 
				Variables.COLLATION_CONNECTION.getSessionValue(ssConnection);

		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.COLLATION_CONNECTION), results);
		assertTrue(results.hasResults());
		assertEquals(origCollation, results.getSingleColumnValue(1, 1));

		String utfCollation = "utf8_unicode_ci";
		ssConnection.setSessionVariable(CollationSessionVariableHandler.VARIABLE_NAME, utfCollation);
		assertEquals(utfCollation,
				Variables.COLLATION_CONNECTION.getSessionValue(ssConnection));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.COLLATION_CONNECTION),
				results);
		assertTrue(results.hasResults());
		assertEquals(utfCollation, results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSelectAllOperation(db, BroadcastDistributionModel.SINGLETON,
				"select @@session.collation_connection"), results);
		assertTrue(results.hasResults());
		assertEquals(utfCollation, results.getSingleColumnValue(1, 1));

		ssConnection.setSessionVariable(CollationSessionVariableHandler.VARIABLE_NAME, origCollation);
		assertEquals(origCollation, 
				Variables.COLLATION_CONNECTION.getSessionValue(ssConnection));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(Variables.COLLATION_CONNECTION), results);
		assertTrue(results.hasResults());
		assertEquals(origCollation, results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSelectAllOperation(db, BroadcastDistributionModel.SINGLETON,
				"select @@session.collation_connection"), results);
		assertTrue(results.hasResults());
		assertEquals(origCollation, results.getSingleColumnValue(1, 1));
	}

	@Test
	public void setInvalidCollation() throws Throwable {
		QueryStepOperation step1op1 = new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null,
				CollationSessionVariableHandler.VARIABLE_NAME, "latin1_junk_ci");
		QueryStep step1 = new QueryStep(sg, step1op1);
		try {
			plan.addStep(step1).executeStep(ssConnection, new MysqlTextResultChunkProvider());
			fail("Expected exception not thrown");
		} catch (PEException e) {
            String receivedMessage = e.getMessage();
            assertTrue("received incorrect error message: "+receivedMessage, receivedMessage.contains("not a supported collation"));
		} catch (Throwable t) {
			fail("Wrong exception thrown");
		}
	}

	/** PE-1154 */
	@Test
	public void setLongQueryTime() throws Throwable {
		assertEquals(new Double(10.0), Variables.LONG_QUERY_TIME.getGlobalValue(ssConnection));
		Variables.LONG_QUERY_TIME.setGlobalValue("25.5");
		assertEquals(new Double(25.5), Variables.LONG_QUERY_TIME.getGlobalValue(ssConnection));
	}

	/** PE-1156 */
	@SuppressWarnings("unchecked")
	@Test
	public void setGroupConcatMaxLen() throws Throwable {
		VariableHandler<Long> var = (VariableHandler<Long>) Variables.lookup("group_concat_max_len", true); 
		assertEquals(new Long(1024), var.getSessionValue(ssConnection));
		var.setSessionValue(ssConnection, "5");
		assertEquals(new Long(5), var.getSessionValue(ssConnection)); 
	}

	/** PE-1128 */
	@Test
	public void setAutocommit() throws Throwable {
		assertEquals(Boolean.TRUE, Variables.AUTOCOMMIT.getSessionValue(ssConnection));
		Variables.AUTOCOMMIT.setSessionValue(ssConnection, "0");
		assertEquals(Boolean.FALSE, Variables.AUTOCOMMIT.getSessionValue(ssConnection));
		Variables.AUTOCOMMIT.setSessionValue(ssConnection, "ON");
		assertEquals(Boolean.TRUE, Variables.AUTOCOMMIT.getSessionValue(ssConnection));
		Variables.AUTOCOMMIT.setSessionValue(ssConnection, "OFF");
		assertEquals(Boolean.FALSE, Variables.AUTOCOMMIT.getSessionValue(ssConnection));
		
		
		final String variableName = "autocommit";

		final String expectedErrorMessage = "Invalid value given for the AUTOCOMMIT variable.";
		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				Variables.AUTOCOMMIT.setSessionValue(ssConnection, null);
			}
		}.assertException(PEException.class, expectedErrorMessage);

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				Variables.AUTOCOMMIT.setSessionValue(ssConnection, "");
			}
		}.assertException(PEException.class, expectedErrorMessage);

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				Variables.AUTOCOMMIT.setSessionValue(ssConnection, "2");
			}
		}.assertException(PEException.class, expectedErrorMessage);
	}

	@Test
	public void setTimeZone() throws Throwable {
		// make sure the default values are properly set
		// now that we support scopes - we should toss over default_time_zone in favor of time_zone with a global scope
		
		String peTimeZoneDefault = Variables.TIME_ZONE.getGlobalValue(ssConnection);		
		assertEquals("+00:00", peTimeZoneDefault);
		assertEquals("+00:00", Variables.TIME_ZONE.getSessionValue(ssConnection)); 

		// change the session variable
		Variables.TIME_ZONE.setSessionValue(ssConnection, "+05:00");
		assertEquals("+05:00", Variables.TIME_ZONE.getSessionValue(ssConnection));

		// change the default PE time zone
		Variables.TIME_ZONE.setGlobalValue("-09:00");
		// new connection should have new default
		SSConnectionProxy conProxy = new SSConnectionProxy();
		try {
			SSConnection ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
			SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
			ssConnection.startConnection(new UserCredentials(bootHost.getProperties()));
			assertEquals("-09:00",  Variables.TIME_ZONE.getSessionValue(ssConnection));
		} finally {
			conProxy.close();
		}
	}

	@Test
	public void getLiteralSessionVariableTest() throws PEException {
        assertEquals("YES", Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo("have_innodb").getHandler()
				.getValue(ssConnection, "have_innodb"));
	}

	@Test(expected = PEException.class)
	public void setLiteralSessionVariableFailTest() throws Throwable {
        Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo("have_innodb").getHandler()
				.setValue(ssConnection, "have_innodb", "NO");
	}

	@Test
	public void setGroupServiceVariableTest() throws PEException {
        HostService hostService = Singletons.require(HostService.class);
		hostService.setGlobalVariable(catalogDAO, "group_service", "HaZeLCaST");
		assertEquals("HaZeLCaST", hostService.getGlobalVariable(catalogDAO, "group_service"));
		hostService.setGlobalVariable(catalogDAO, "group_service", "Localhost");
		assertEquals("Localhost", hostService.getGlobalVariable(catalogDAO, "group_service"));
	}

	@Test(expected = PEException.class)
	public void setGroupServiceVariableFailTest() throws PEException {
        Singletons.require(HostService.class).setGlobalVariable(catalogDAO, "group_service", "InvalidValue");

	}

	private void executeQuery(QueryStepOperation qso, DBResultConsumer results) throws Throwable {
		QueryPlan qp = new QueryPlan();
		QueryStep step1 = new QueryStep(sg, qso);
		qp.addStep(step1);
		qp.executeStep(ssConnection, results);
		qp.close();
	}

}
