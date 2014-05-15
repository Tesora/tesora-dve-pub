// OS_STATUS: public
package com.tesora.dve.mysqlapi;

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
