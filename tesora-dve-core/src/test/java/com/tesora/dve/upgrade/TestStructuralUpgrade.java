// OS_STATUS: public
package com.tesora.dve.upgrade;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.sql.transexec.CatalogHelper;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.standalone.PETest;

// not a PETest because we're going to flush the dve database 
// many times during this (twice per upgrade)
// the way we do this is we install n, get table state, install n - 1, upgrade to n, get table state, and compare
// then repeat for the next pair
// 
public class TestStructuralUpgrade {

	static DBHelper helper;
	static CatalogHelper catalogHelper;
	static TestHost testHost;
	
	@BeforeClass
	public static void createDBConnection() throws Throwable {
        testHost = TestHost.startServicesTransient(PETest.class);
        catalogHelper = new CatalogHelper(Singletons.require(HostService.class).getProperties());			
		catalogHelper.deleteCatalog();
		catalogHelper.createCatalogDB();
        helper = new DBHelper(Singletons.require(HostService.class).getProperties());
		helper.connect();
	}
	
	@AfterClass
	public static void destroyDBConnection() {
		catalogHelper.close();
		helper.disconnect();
		TestHost.stopServices();
	}
	
	@Test
	public void testUpgrade() throws Throwable {
		List<CatalogVersion> versions = CatalogVersions.getVersionHistory();

		CatalogVersion firstTested = CatalogVersions.getOldestUpgradeSupported().getCatalogVersion();
		
		CatalogVersion prev = firstTested;
		for(CatalogVersion current : versions) {
			if(current.getSchemaVersion() <= firstTested.getSchemaVersion())
				continue;

			String[] currentCommands = UpgradeTestUtils.getGoldVersion(current.getSchemaVersion());
			installSchema(currentCommands);
			List<String> tableNames = findTableNames(currentCommands);
			SchemaState currentState = buildState(tableNames);
			String[] prevCommands = UpgradeTestUtils.getGoldVersion(prev.getSchemaVersion());
			installSchema(prevCommands);
			current.upgrade(helper);
			SchemaState upgradedState = buildState(tableNames);
			String diffs = currentState.differs(upgradedState);
			if (diffs != null)
				fail(diffs);
			prev = current;
		}
	}

	private void installSchema(String[] commands) throws Throwable {
		catalogHelper.deleteCatalog();
		catalogHelper.createCatalogDB();
		// restart the CatalogDAO Factory
		testHost.startDAO();
		CatalogSchemaGenerator.installSchema(helper, commands);
		helper.connect();
	}
	
	private SchemaState buildState(List<String> tabNames) throws Throwable {
		SchemaState ss = new SchemaState();
		ss.build(helper,tabNames);
		return ss;
	}
	
	private List<String> findTableNames(String[] commands) throws Throwable {
		ArrayList<String> out = new ArrayList<String>();
		String header = "create table";
		for(String c : commands) {
			if (c.startsWith(header)) {
				int nextSpace = c.indexOf(" ", header.length() + 2);
				String tabName = c.substring(header.length() + 1, nextSpace);
				out.add(tabName.trim());
			}
		}
		return out;
	}
}
