// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import java.util.Properties;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.server.bootstrap.Host;

/**
 * This class is for tests that need some things from Host but not the full
 * BootstrapHost. Specifically, it will cause the DBNative class to be
 * initialized so that Host.getDBNative() will work. Also the CatalogDAO 
 * infrastructure will be started
 * 
 */
public class TestHost extends Host {

	public TestHost(Properties props) {
		super(props, false /* startCatalog */);
	}

	public static <T> TestHost startServicesTransient(Class<T> clazz) throws PEException {
		Properties props = PEFileUtils.loadPropertiesFile(clazz, PEConstants.CONFIG_FILE_NAME);
		return new TestHost(props);
	}
	
	public static <T> TestHost startServices(Class<T> clazz) throws PEException {
		Properties props = PEFileUtils.loadPropertiesFile(clazz, PEConstants.CONFIG_FILE_NAME);
		return startServices(props);
	}

	private static TestHost startServices(Properties props) throws PEException {
		GroupManager.initialize(props);
		CatalogDAOFactory.setup(props);
		return new TestHost(props);
	}
	
	public static void stopServices() {
		CatalogDAOFactory.shutdown();
		GroupManager.shutdown();
	}
	
	public void startDAO() throws PEException {
		CatalogDAOFactory.setup(getProperties());
	}
}
