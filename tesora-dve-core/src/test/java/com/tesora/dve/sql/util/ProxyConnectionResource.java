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


import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.comms.client.messages.DisconnectRequest;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.messages.ExecuteRequestExecutor;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.worker.DBConnectionParameters;
import com.tesora.dve.worker.MysqlTextResultChunkProvider;
import com.tesora.dve.worker.UserCredentials;

public class ProxyConnectionResource extends ConnectionResource {

	private DBConnectionParameters connectionParams;
	private SSConnection proxy;
	
	private String userName = null;
	private String password = null;
	
	private boolean lastHadWarning = false;
	
	public ProxyConnectionResource() throws Throwable {
		this(null,null);
	}
	
	public ProxyConnectionResource(String userName, String password) throws Throwable {
		super(null);
		this.userName = userName;
		this.password = password;
        Properties props = (Properties) Singletons.require(HostService.class).getProperties().clone();
		if (userName != null)
			props.setProperty(DBHelper.CONN_USER, userName);
		if (password != null)
			props.setProperty(DBHelper.CONN_PASSWORD, password);
		connectionParams = new DBConnectionParameters(props);
		proxy = null;
		connect();			
	}
	
	protected ProxyConnectionResource(ProxyConnectionResource pcr) throws Throwable {
		super(pcr);
		this.userName = pcr.userName;
		this.password = pcr.password;
		this.connectionParams = new DBConnectionParameters(pcr.connectionParams);
		this.proxy = null;
		connect();
	}
	
	@Override
	public void connect() throws Throwable {
		if (proxy != null) return;
		proxy = new SSConnection();
		try {
			proxy.startConnection(new UserCredentials(connectionParams.getUserid(), connectionParams.getPassword()));
			executePostConnectCmds();
		} catch ( Throwable t ) {
			proxy.close();
			throw t;
		}
		
	}
	
	@Override
	public void disconnect() throws Throwable {
		if (proxy == null) 
			return;
		
		DisconnectRequest discon = new DisconnectRequest();
		proxy.executeMessageDirect(discon);
		proxy.close();
		proxy = null;
	}

	public ResourceResponse execute(LineInfo info, final byte[] stmt) throws Throwable {
		if (info != null)
			SchemaTest.echo("Executing@" + info + ": '" + stmt + "'");
		else
			SchemaTest.echo("Executing: '" + stmt + "'");

//		final NativeCharSet clientCharSet = MysqlNativeCharSet.UTF8;
		final MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		proxy.executeInContext(new Callable<Void>() {
			@SuppressWarnings("synthetic-access")
			@Override
			public Void call() throws Exception {
				try {
					ExecuteRequestExecutor.execute(proxy, results, stmt);
				} catch (PEException e) {
					throw e;
				} catch (SchemaException se) {
					throw se;
				} catch (Throwable e) {
					throw new PEException("Test statement encountered exception", e);
				}
				return null;
			}
		});
		
		lastHadWarning = results.getWarnings() > 0;

		return new ProxyConnectionResourceResponse(results);
	}

	@Override
	public ResourceResponse execute(LineInfo info, String stmt) throws Throwable {
		if (info != null)
			SchemaTest.echo("Executing@" + info + ": '" + stmt + "'");
		else
			SchemaTest.echo("Executing: '" + stmt + "'");
		
		return execute(info, stmt.getBytes());
	}
	
	@Override
	public ResourceResponse fetch(LineInfo info, String stmt)
			throws Throwable {
		return execute(info, stmt);
	}

	@Override
	public ExceptionClassification classifyException(Throwable t) {
		if (t.getMessage() == null)
			return null;
		
		String msg = t.getMessage().trim();
		if (StringUtils.containsIgnoreCase(msg, "No such Table:") || 
				StringUtils.containsIgnoreCase(msg, "No such Column:"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(msg, "No value found for required") && 
				StringUtils.endsWithIgnoreCase(msg, "and no default specified"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(msg, "Unsupported statement kind for planning:") && 
				StringUtils.endsWithIgnoreCase(msg, "RollbackTransactionStatement"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(msg, "Data Truncation:"))
			return ExceptionClassification.OUT_OF_RANGE;
		if (StringUtils.containsIgnoreCase(msg, "Unable to parse") ||
				StringUtils.containsIgnoreCase(msg, "Unable to build plan") ||
				StringUtils.containsIgnoreCase(msg, "Parsing FAILED:"))
			return ExceptionClassification.SYNTAX;
		if (StringUtils.containsIgnoreCase(msg, "Duplicate entry"))
			return ExceptionClassification.DUPLICATE;
		if (StringUtils.containsIgnoreCase(msg, "option not supported"))
			return ExceptionClassification.UNSUPPORTED_OPERATION;
		return null;
	}
	
	@Override
	public ConnectionResource getNewConnection() throws Throwable {
		return new ProxyConnectionResource(this);
	}

	@Override
	public boolean isConnected() throws Throwable {
		return proxy != null;
	}

	@Override
	public String describe() {
		if (userName == null && password == null)
			return "null/null";
		return userName + "/" + password;
	}

	@Override
	public Object prepare(LineInfo info, String stmt) throws Throwable {
		throw new PEException("prepare() method not implemented for ProxyConnectionResource");
	}

	@Override
	public ResourceResponse executePrepared(Object ps, List<Object> parameters) throws Throwable {
		throw new PEException("executePrepared() method not implemented for ProxyConnectionResource");
	}

	@Override
	public void destroyPrepared(Object ps) throws Throwable {
		throw new PEException("destroyPrepared() method not implemented for ProxyConnectionResource");		
	}
	
	@Override
	public boolean hasWarnings() {
		return lastHadWarning;
	}
	
}
