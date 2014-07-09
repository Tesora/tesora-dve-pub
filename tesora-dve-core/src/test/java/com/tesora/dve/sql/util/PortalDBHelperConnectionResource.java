package com.tesora.dve.sql.util;

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

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.standalone.PETest;

public class PortalDBHelperConnectionResource extends
		DBHelperConnectionResource {

	public PortalDBHelperConnectionResource() throws Throwable {
		super();
	}

	public PortalDBHelperConnectionResource(Properties urlConnectOptions) throws Throwable {
		super(urlConnectOptions);
	}

	public PortalDBHelperConnectionResource(String userName, String password) throws Throwable {
		super(userName, password);
	}
	
	protected PortalDBHelperConnectionResource(PortalDBHelperConnectionResource other) throws Throwable {
		super(other);
	}
	
	@Override
	public JdbcConnectParams getConnectParams() throws Throwable {
		if (connParams != null) return connParams;
		Properties catalogProps = TestCatalogHelper.getTestCatalogProps(PETest.class);
		String portalPort = catalogProps.getProperty(PEConstants.MYSQL_PORTAL_PORT_PROPERTY, PEConstants.MYSQL_PORTAL_DEFAULT_PORT);

		PEUrl peurl = PEUrl.fromUrlString(catalogProps.getProperty(DBHelper.CONN_URL));
		peurl.setPort(portalPort);
		JdbcConnectParams jcp = new JdbcConnectParams(peurl.getURL(),
				catalogProps.getProperty(DBHelper.CONN_USER),
				catalogProps.getProperty(DBHelper.CONN_PASSWORD));

		return jcp;
	}

	@Override
	public ConnectionResource getNewConnection() throws Throwable {
		return new PortalDBHelperConnectionResource(this);
	}
	
	@Override
	public ExceptionClassification classifyException(Throwable t) {
		String msg = t.getMessage().trim();
		if (msg == null)
			return null;
		if (StringUtils.endsWithIgnoreCase(msg, "expected exception"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(msg, "No such Table:") ||
				StringUtils.containsIgnoreCase(msg, "No such Table(s)") ||
				StringUtils.containsIgnoreCase(msg, "No such Column:"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(msg, "Unsupported statement kind for planning:") && 
				StringUtils.endsWithIgnoreCase(msg, "RollbackTransactionStatement"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(msg, "Data Truncation:"))
			return ExceptionClassification.OUT_OF_RANGE;
		if (StringUtils.containsIgnoreCase(msg, "ParserException") ||
				StringUtils.containsIgnoreCase(msg, "Exception: Unable to build plan") ||
				StringUtils.containsIgnoreCase(msg, "Parsing Failed:"))
			return ExceptionClassification.SYNTAX;
		if (StringUtils.containsIgnoreCase(msg, "Duplicate entry"))
			return ExceptionClassification.DUPLICATE;
		// this one should be removed ASAP we know we have a parser error when we get this
		if (StringUtils.containsIgnoreCase(msg, "java.lang.NullPointerException"))
			return ExceptionClassification.SYNTAX;
		return super.classifyException(t);
	}
}
