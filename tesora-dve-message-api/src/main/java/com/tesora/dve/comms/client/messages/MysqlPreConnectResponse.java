// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public class MysqlPreConnectResponse extends PreConnectResponse {

	private static final long serialVersionUID = 1L;

	protected String salt;
	protected int charset;
	protected String pluginData;
	protected long serverCapabilities;
	
	public void setSalt( String salt ) {
		this.salt = salt;
	}
	
	public String getSalt() {
		return this.salt;
	}

	public void setCharset( int charset ) {
		this.charset = charset;
	}
	
	public int getCharset() {
		return this.charset;
	}

	public void setServerCapabilities( long serverCap ) {
		this.serverCapabilities = serverCap;
	}
	
	public long getServerCapabilities() {
		return serverCapabilities;
	}

	public void setPluginData( String pluginData ) {
		this.pluginData = pluginData;
	}
	
	public String getPluginData() {
		return this.pluginData;
	}
	
	@Override
	public MessageType getMessageType() {
		return MessageType.MYSQL_PRECONNECT_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}
}
