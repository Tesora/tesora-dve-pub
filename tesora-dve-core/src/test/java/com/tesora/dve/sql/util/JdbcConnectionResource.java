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

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import com.tesora.dve.common.PEUrl;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;

public abstract class JdbcConnectionResource extends ConnectionResource {
	protected JdbcConnectParams connParams;
	
	public JdbcConnectionResource() {
		super(null);
	}
	
	protected JdbcConnectionResource(JdbcConnectionResource other) {
		super(other);
		connParams = new JdbcConnectParams(other.connParams);
	}
	
	private JdbcConnectionResourceResponse executeQuery(String stmt) throws Throwable {
		Statement sqlStatement = getStatement();
		boolean values = sqlStatement.execute(stmt);
		if (values) {
			return new JdbcConnectionResourceResponse(sqlStatement.getResultSet(),null,0);
		}
		return new JdbcConnectionResourceResponse(null,new Long(sqlStatement.getUpdateCount()), 0);
	}

	public void initialize(JdbcConnectParams jdbcConnParams) {
		this.connParams = jdbcConnParams;
	}
	
	public String getUrl() throws Throwable {
		return connParams.getUrl();
	}

	public PEUrl getPEUrl() throws Throwable {
		return connParams.getPEUrl();
	}
	
	public String getUserid() {
		return connParams.getUserid();
	}
	
	public String getPassword() {
		return connParams.getPassword();
	}
	
	@Override
	public ResourceResponse execute(LineInfo info, String stmt)	throws Throwable {
		return executeQuery(stmt);
	}

	@Override
	public ResourceResponse fetch(LineInfo info, String stmt)
			throws Throwable {
		return executeQuery(stmt);
	}

	@Override
	public Object prepare(LineInfo info, String stmt) throws Throwable {
		throw new PEException("prepare() method not implemented for JdbcConnectionResource");
	}

	@Override
	public ResourceResponse executePrepared(Object id, List<Object> parameters) throws Throwable {
		throw new PEException("executePrepared() method not implemented for JdbcConnectionResource");
	}

	@Override
	public void destroyPrepared(Object id) throws Throwable {
		throw new PEException("destroyPrepared() method not implemented for JdbcConnectionResource");		
	}
	
	public abstract Statement getStatement() throws Throwable;
	
	public Connection getConnection() throws Throwable {
		return getStatement().getConnection();
	}
}
