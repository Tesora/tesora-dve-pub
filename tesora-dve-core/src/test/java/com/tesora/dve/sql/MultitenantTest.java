// OS_STATUS: public
package com.tesora.dve.sql; // NOPMD by doug on 04/12/12 12:05 PM



import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public abstract class MultitenantTest extends SchemaTest {

	protected static final PEDDL testDDL =
			new PEDDL("mtdb", 
					new StorageGroupDDL("mttsg",5,"pg"),
					"database").withTemplate("mttemp", true).withMTMode(MultitenantMode.ADAPTIVE);		
		
	@BeforeClass
	public static void setup() throws Throwable {
		projectSetup(testDDL);
		bootHost = BootstrapHost.startServices(PETest.class);
		setTemplateModeOptional();
	}

	protected ConnectionResource rootConnection = null;
	protected ConnectionResource tenantConnection = null;
	protected static final String mtuserName = "dguser";
	protected static final String mtuserAccess = "localhost";
	protected static final String[] tenantNames = new String[] { "mtt", "stt" };
	protected static final String[] adaptiveTenants = new String[] { "at1", "at2" };

	@Before
	public void setupTest() throws Throwable {
		rootConnection = new PortalDBHelperConnectionResource();
		rootConnection.execute("drop template if exists mttemp");
		rootConnection.execute(new TemplateBuilder("mttemp")
		.withRequirement("create range block_range (int) persistent group #sg#")
		.withRangeTable(".*", "block_range", "___mtid")
		.toCreateStatement());
		removeUser(rootConnection,mtuserName,mtuserAccess);
		testDDL.getPersistentGroup().create(rootConnection);
		rootConnection.execute("create user '" + mtuserName + "'@'" + mtuserAccess + "' identified by '" + mtuserName + "'");
		tenantConnection = new PortalDBHelperConnectionResource(mtuserName,mtuserName);
	}

	@After
	public void teardownTest() throws Throwable {
		if(tenantConnection != null)
			tenantConnection.disconnect();

		if(rootConnection != null) {
			removeUser(rootConnection,mtuserName,mtuserAccess);
			// also remove the tenant
			rootConnection.execute("drop tenant " + tenantNames[0]);
			rootConnection.execute("drop database if exists " + tenantNames[1]);
			for(int i = 0; i < adaptiveTenants.length; i++)
				rootConnection.execute("drop database if exists " + adaptiveTenants[i]);
		}
		if(testDDL != null)
			testDDL.destroy(rootConnection);

        WorkerGroupFactory.shutdown(bootHost.getWorkerManager());
		
		if(rootConnection != null)
			rootConnection.disconnect();
	}

	protected void createTenant(int which) throws Throwable {
		createTenant(tenantNames[which],(which == 0 ? "tenant" : "database"));
	}

	protected void createAdaptiveTenant(int which) throws Throwable {
		createTenant(adaptiveTenants[which],"database");
	}

	protected void createTenant(String tenantName, String createCommand) throws Throwable {
		if ("database".equals(createCommand))
			rootConnection.execute("create database " + tenantName);
		else
			rootConnection.execute("create " + createCommand + " " + tenantName + " '" + tenantName + "'");
		try {
			rootConnection.execute("grant all on " + tenantName + ".* to '" + mtuserName + "'@'" + mtuserAccess + "' identified by '" + mtuserName + "'");
		} catch (Throwable t) {
			throw new PEException("unable to grant access to " + tenantName + " for mtuser", t);
		}
	}

	protected void becomeLT() throws Throwable {
		rootConnection.execute("use mtdb");		
	}

	protected void becomeL() throws Throwable {
		rootConnection.execute("use " + PEConstants.LANDLORD_TENANT);		
	}

	protected void setContext(String tn) {
		SSConnectionProxy.setOperatingContext(this.getClass().getSimpleName() + "." + tn);
	}

	protected void assertAutoInc(ConnectionResource tc, String cts) throws Throwable {
		String ai = "AUTO_INCREMENT";
		int first = cts.indexOf(ai);
		assertTrue("should have column autoinc",first > -1);
		int second = cts.indexOf(ai,first + ai.length());
		assertTrue("should have table autoinc",second > -1);
		tc.assertResults("show columns in altest like 'cola'", br(nr,"cola","int(11)","NO","PRI",null,"auto_increment"));
	}
	
}
