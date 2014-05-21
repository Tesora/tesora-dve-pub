// OS_STATUS: public
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
		ssConnection.startConnection(new UserCredentials(PEConstants.ROOT, PEConstants.PASSWORD));
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
		executeQuery(new QueryStepGetSessionVariableOperation("character_set_client"), results);
		assertTrue(results.hasResults());
		assertEquals("latin1", results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null, "character_set_client",
				"utf8"), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation("character_set_client"), results);
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
		executeQuery(new QueryStepGetSessionVariableOperation("dynamic_policy"), results);
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
		executeQuery(new QueryStepGetSessionVariableOperation(PERSISTENT_GROUP_VARIABLE), results);
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
		executeQuery(new QueryStepGetSessionVariableOperation("Persistent_Group"), results);
		assertTrue(results.hasResults());
		assertEquals(1, results.getNumRowsAffected());
	}

	@Test
	public void clientCharSet() throws Throwable {
		String origCharSet = ssConnection.getSessionVariable(ClientCharSetSessionVariableHandler.VARIABLE_NAME);
		String newCharset = "utf8";
		if (!origCharSet.toLowerCase().equals("utf8"))
			newCharset = "latin1";

		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSetScopedVariableOperation(VariableScopeKind.SESSION, null,
				ClientCharSetSessionVariableHandler.VARIABLE_NAME, newCharset), results);
		assertFalse(results.hasResults());

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(ClientCharSetSessionVariableHandler.VARIABLE_NAME), results);
		assertTrue(results.hasResults());
		assertEquals(newCharset, results.getSingleColumnValue(1, 1));
	}

	@Test
	public void setValidCollation() throws Throwable {
		String origCollation = ssConnection.getSessionVariable(CollationSessionVariableHandler.VARIABLE_NAME);

		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(CollationSessionVariableHandler.VARIABLE_NAME),
				results);
		assertTrue(results.hasResults());
		assertEquals(origCollation, results.getSingleColumnValue(1, 1));

		String utfCollation = "utf8_unicode_ci";
		ssConnection.setSessionVariable(CollationSessionVariableHandler.VARIABLE_NAME, utfCollation);
		assertEquals(utfCollation, ssConnection.getSessionVariable(CollationSessionVariableHandler.VARIABLE_NAME));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(CollationSessionVariableHandler.VARIABLE_NAME),
				results);
		assertTrue(results.hasResults());
		assertEquals(utfCollation, results.getSingleColumnValue(1, 1));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepSelectAllOperation(db, BroadcastDistributionModel.SINGLETON,
				"select @@session.collation_connection"), results);
		assertTrue(results.hasResults());
		assertEquals(utfCollation, results.getSingleColumnValue(1, 1));

		ssConnection.setSessionVariable(CollationSessionVariableHandler.VARIABLE_NAME, origCollation);
		assertEquals(origCollation, ssConnection.getSessionVariable(CollationSessionVariableHandler.VARIABLE_NAME));

		results = new MysqlTextResultChunkProvider();
		executeQuery(new QueryStepGetSessionVariableOperation(CollationSessionVariableHandler.VARIABLE_NAME),
				results);
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
		assertEquals("10.0", ssConnection.getSessionVariable("long_query_time"));
		ssConnection.setSessionVariable("long_query_time", "25.5");
		assertEquals("25.5", ssConnection.getSessionVariable("long_query_time"));
	}

	/** PE-1156 */
	@Test
	public void setGroupConcatMaxLen() throws Throwable {
		assertEquals("1024", ssConnection.getSessionVariable("group_concat_max_len"));
		ssConnection.setSessionVariable("group_concat_max_len", "5");
		assertEquals("5", ssConnection.getSessionVariable("group_concat_max_len"));
	}

	/** PE-1128 */
	@Test
	public void setAutocommit() throws Throwable {
		final String variableName = "autocommit";

		assertEquals("1", ssConnection.getSessionVariable(variableName));
		ssConnection.setSessionVariable(variableName, "0");
		assertEquals("0", ssConnection.getSessionVariable(variableName));
		ssConnection.setSessionVariable(variableName, "ON");
		assertEquals("ON", ssConnection.getSessionVariable(variableName));
		ssConnection.setSessionVariable(variableName, "OFF");
		assertEquals("OFF", ssConnection.getSessionVariable(variableName));

		final String expectedErrorMessage = "Invalid value given for the AUTOCOMMIT variable.";
		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				ssConnection.setSessionVariable(variableName, null);
			}
		}.assertException(PEException.class, expectedErrorMessage);

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				ssConnection.setSessionVariable(variableName, "");
			}
		}.assertException(PEException.class, expectedErrorMessage);

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				ssConnection.setSessionVariable(variableName, "2");
			}
		}.assertException(PEException.class, expectedErrorMessage);
	}

	@Test
	public void setTimeZone() throws Throwable {
		// make sure the default values are properly set
        String peTimeZoneDefault = Singletons.require(HostService.class).getGlobalVariable(catalogDAO, "default_time_zone");
		assertEquals("+00:00", peTimeZoneDefault);
		assertEquals("+00:00", ssConnection.getSessionVariable("time_zone"));

		// change the session variable
		ssConnection.setSessionVariable("time_zone", "+05:00");
		assertEquals("+05:00", ssConnection.getSessionVariable("time_zone"));

		// change the default PE time zone
        Singletons.require(HostService.class).setGlobalVariable(catalogDAO, "default_time_zone", "-09:00");
		// new connection should have new default
		SSConnectionProxy conProxy = new SSConnectionProxy();
		try {
			SSConnection ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
			SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
			ssConnection.startConnection(new UserCredentials(PEConstants.ROOT, PEConstants.PASSWORD));
			assertEquals("-09:00", ssConnection.getSessionVariable("time_zone"));
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
