// OS_STATUS: public
package com.tesora.dve.common;

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

import java.util.Properties;

import org.junit.Test;

import com.tesora.dve.common.PEUrl;
import com.tesora.dve.exceptions.PEException;

public class PEUrlTest {

	@Test
	public void parseURLPositiveTest() throws PEException {

		String urlString = "jdbc:mysql://localhost:3306/path1/path2?opt1=optvalue";
		PEUrl url = PEUrl.fromUrlString(urlString);
		assertEquals(urlString, url.getURL());
		assertEquals("localhost:3306", url.getAuthority());

		urlString = "jdbc:mysql://localhost:3306";
		url = PEUrl.fromUrlString(urlString);
		assertEquals(urlString, url.getURL());
		
		urlString = "jdbc:mysql://localhost";
		url = PEUrl.fromUrlString(urlString);
		assertEquals(urlString, url.getURL());
		assertEquals("localhost", url.getAuthority());

		urlString = "jdbc:mysql://localhost?opt1=optvalue";
		urlString = "jdbc:mysql://localhost?opt1=optvalue";
		url = PEUrl.fromUrlString(urlString);
		assertEquals(urlString, url.getURL());

		urlString = "jdbc:mysql://localhost:3306?opt1=opt1value&opt2=opt2value";
		url = PEUrl.fromUrlString(urlString);
		assertEquals(urlString, url.getURL());

		String baseUrl = "jdbc:mysql://localhost:3306";
		urlString = baseUrl + "?opt2=opt2value&opt1=opt1value";
		Properties queryProps = new Properties();
		url = PEUrl.fromUrlString(baseUrl);
		queryProps.setProperty("opt1", "opt1value");
		queryProps.setProperty("opt2", "opt2value");
		url.setQueryOptions(queryProps);
		assertEquals(urlString, url.getURL());
	}

	@Test(expected = PEException.class)
	public void parseURLNegativeTest1() throws PEException {
		PEUrl.fromUrlString("jdbc:mysql:somestuff//localhost:3306?opt1=optvalue");
	}

	@Test(expected = PEException.class)
	public void parseURLNegativeTest2() throws PEException {

		PEUrl.fromUrlString("jdbc://localhost:6800?opt1=optvalue");
	}

	@Test(expected = PEException.class)
	public void parseURLNegativeTest3() throws PEException {

		PEUrl.fromUrlString("jdbc:mysql//localhost");
	}
	
	@Test(expected = PEException.class)
	public void parseURLNegativeTest4() throws PEException {

		PEUrl.fromUrlString("jdbc:mysql://localhost:3306/?opt1=optvalue&opt2");
	}

	@Test(expected = PEException.class)
	public void parseURLNegativeTest5() throws PEException {
		new PEUrl(new Properties(), "", false);
	}
	
	@Test(expected = PEException.class)
	public void parseURLNegativeTest6() throws PEException {
		PEUrl.fromUrlString("jdbc:mysql://");
	}
	
	@Test
	public void fromConnectStringTest() throws PEException {
		String connectString = "host=localhost;port=3306;dbname=parelastic";
		PEUrl url = PEUrl.fromConnectString(connectString);
		assertEquals("jdbc:mysql://localhost:3306/parelastic",
				url.getURL());

		connectString = "host=localhost";
		url = PEUrl.fromConnectString(connectString);
		assertEquals("jdbc:mysql://localhost", url.getURL());

		connectString = "host=foo;port=7000;dbname=bar";
		url = PEUrl.fromConnectString(connectString);
		assertEquals("jdbc:mysql://foo:7000/bar", url.getURL());

	}

	@Test(expected = PEException.class)
	public void fromConnectStringFailTest1() throws PEException {
		String connectString = "hostname=localhost;port=3306;dbname=parelastic";
		PEUrl.fromConnectString(connectString);
	}

	@Test(expected = PEException.class)
	public void fromConnectStringFailTest2() throws PEException {

		String connectString = "host=localhost;port";
		PEUrl.fromConnectString(connectString);
	}

	@Test(expected = PEException.class)
	public void fromConnectStringFailTest3() throws PEException {

		String connectString = "host=;port=;";
		PEUrl.fromConnectString(connectString);
	}

	@Test
	public void parseURLConstructTest() throws PEException {

		PEUrl url = new PEUrl();
		assertFalse(url.isInitialized());

		url.setHost("foo").setPort(1234).setProtocol("jdbc")
				.setSubProtocol("mysql");
		assertTrue(url.isInitialized());
		assertEquals("jdbc:mysql://foo:1234", url.getURL());

		url.setQueryOption("opt1", "opt1value").setQueryOption("opt2",
				"opt2value");
		assertEquals(
				"jdbc:mysql://foo:1234?opt2=opt2value&opt1=opt1value",
				url.getURL());

		url = new PEUrl();
		assertFalse(url.isInitialized());
		url.setHost("foo").setProtocol("jdbc").setSubProtocol("mysql");
		assertTrue(url.isInitialized());
		assertEquals("jdbc:mysql://foo", url.getURL());

		url = new PEUrl();
		assertFalse(url.isInitialized());
		assertEquals("jdbc:mysql://localhost:3306", url.createMysqlDefaultURL().getURL());

		url = new PEUrl().createMysqlDefaultURL();
		assertTrue(url.isInitialized());
		assertEquals("jdbc:mysql://localhost:3306", url.getURL());

		Properties props = new Properties();
		props.setProperty("jdbc.url",
				"jdbc:mysql://foo:1234?opt2=opt2value&opt1=opt1value");
		url = new PEUrl(props, "jdbc");
		assertTrue(url.isInitialized());
		assertEquals(
				"jdbc:mysql://foo:1234?opt2=opt2value&opt1=opt1value",
				url.getURL());

		props = new Properties();
		props.setProperty("jdbc.host", "localhost");
		props.setProperty("jdbc.type", "mysql");
		props.setProperty("jdbc.dbname", "project");
		url = new PEUrl(props, "jdbc");
		assertTrue(url.isInitialized());
		assertEquals("jdbc:mysql://localhost/project", url.getURL());

		// test that useDefaults is working by sending in empty Properties
		props = new Properties();
		url = new PEUrl(props, null, true);
		assertTrue(url.isInitialized());
		assertEquals(new PEUrl().createMysqlDefaultURL().getURL(), url.getURL());
		
	}

}
