// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;

public abstract class QueryStatementBasedRequest extends StatementBasedRequest {

	private static final long serialVersionUID = 1L;
	
	protected static final Charset STRING_DEFAULT_CHARSET = CharsetUtil.UTF_8;

	protected byte[] command;
	protected String charset;

	protected QueryStatementBasedRequest() {
	};

	protected QueryStatementBasedRequest(String stmtId, String command) {
		super(stmtId);
		setCommand(command.getBytes(STRING_DEFAULT_CHARSET));
		setCharset(STRING_DEFAULT_CHARSET.name());
	}

	public QueryStatementBasedRequest(String statementId, byte[] command) {
		super(statementId);
		setCommand(command);
	}

	public byte[] getCommand() {
		return (byte[]) this.command;
	}
	
	public String getCharset() {
		return this.charset;
	}
	
	void setCharset(String charset) {
		this.charset = charset;
	}

	public void setCommand(byte[] command) {
		this.command = command;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer().append(this.getClass().getSimpleName()).append(",")
				.append(getStatementId()).append(",");
		return sb.append(new String(getCommand(), STRING_DEFAULT_CHARSET)).append(")").toString();
	}

}
