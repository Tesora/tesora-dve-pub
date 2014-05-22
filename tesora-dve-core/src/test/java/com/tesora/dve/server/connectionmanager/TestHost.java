package com.tesora.dve.server.connectionmanager;

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
