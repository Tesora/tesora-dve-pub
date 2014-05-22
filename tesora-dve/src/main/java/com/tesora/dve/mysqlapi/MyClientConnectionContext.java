package com.tesora.dve.mysqlapi;

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

import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.db.mysql.libmy.MyHandshakeV10;

public class MyClientConnectionContext {

	private MyHandshakeV10 sgr;
	private ChannelHandlerContext ctx;
	private String connectHost;
	private int connectPort;

	public MyClientConnectionContext(String connectHost, int connectPort) {
		this.connectHost = connectHost;
		this.connectPort = connectPort;
	}
	
	public String getConnectHost() {
		return connectHost;
	}

	void setConnectHost(String connectHost) {
		this.connectHost = connectHost;
	}

	public int getConnectPort() {
		return connectPort;
	}

	void setConnectPort(int connectPort) {
		this.connectPort = connectPort;
	}

	public String getName() {
		return this.getClass().getName();
	}
	
	public void setSgr(MyHandshakeV10 sgr) {
		this.sgr = sgr;
	}

	public String getSaltforPassword() {
		return sgr.getSalt();
	}
	
	public String getPlugInData() {
		return sgr.getPlugInProvidedData();
	}

	public ChannelHandlerContext getCtx() {
		return ctx;
	}

	public void setCtx(ChannelHandlerContext ctx) {
		this.ctx = ctx;
	}
}
