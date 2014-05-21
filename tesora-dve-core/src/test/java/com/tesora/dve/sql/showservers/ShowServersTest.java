// OS_STATUS: public
package com.tesora.dve.sql.showservers;

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
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Properties;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.standalone.PETest;

public class ShowServersTest extends PETest {
	
	private static DBHelper dbHelper;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestCatalogHelper.createTestCatalog(PETest.class, 2);
		bootHost = BootstrapHost.startServices(PETest.class);

        populateMetadata(ShowServersTest.class, Singletons.require(HostService.class).getProperties());
		
		final Properties connectionSettings = PEFileUtils.loadPropertiesFile(ShowServersTest.class, PEConstants.CONFIG_FILE_NAME);
		dbHelper = new DBHelper(
				connectionSettings.getProperty(PEConstants.PROP_JDBC_URL),
				connectionSettings.getProperty(PEConstants.PROP_JDBC_USER),
				connectionSettings.getProperty(PEConstants.PROP_JDBC_PASSWORD)).connect();
	}

	@Test
	public void testShowServers() throws SQLException {
		assertTrue(dbHelper.executeQuery("SHOW SERVERS"));
		assertEquals(2, countResultSetRows(dbHelper.getResultSet()));
	}
	
	@AfterClass
	public static void tearDownBaseAfterClass() throws Exception {
		if(dbHelper != null)
			dbHelper.disconnect();
	}

}
