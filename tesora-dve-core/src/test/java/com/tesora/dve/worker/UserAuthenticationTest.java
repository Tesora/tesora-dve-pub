// OS_STATUS: public
package com.tesora.dve.worker;

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

import com.tesora.dve.db.mysql.portal.protocol.MSPAuthenticateV10MessageMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.SSConnectionAccessor;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.standalone.PETest;

public class UserAuthenticationTest extends PETest {

	static SSConnection ssConn;
	static SSConnectionProxy ssConnProxy;
	
	@BeforeClass
	public static void startUp() throws Exception {
		TestCatalogHelper.createMinimalCatalog(PETest.class);
		bootHost = BootstrapHost.startServices(PETest.class);
		ssConnProxy = new SSConnectionProxy();
		ssConn = SSConnectionAccessor.getSSConnection(ssConnProxy);
		SSConnectionAccessor.setCatalogDAO(ssConn, CatalogDAOFactory.newInstance());
	}

	@AfterClass
	public static void shutdown() throws Exception {
		ssConnProxy.close();
	}

	@Test
	public void testSuccessWithPlaintext() throws PEException {
		new UserCredentials("root", "password").authenticate(ssConn);
	}

	@Test (expected=PEException.class)
	public void testUserFailureWithPlaintext() throws PEException {
		new UserCredentials("baduser", "password").authenticate(ssConn);
	}

	@Test (expected=PEException.class)
	public void testPwdFailureWithPlaintext() throws PEException {
		new UserCredentials("root", "badpassword").authenticate(ssConn);
	}

	@Test
	public void testSuccessWithHashed() throws Exception {
		String hashPass = MSPAuthenticateV10MessageMessage.computeSecurePasswordString("password", ssConn.getHandshake().getSalt());

		new UserCredentials("root", hashPass, false).authenticate(ssConn);
	}

}
