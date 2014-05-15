// OS_STATUS: public
package com.tesora.dve.sql.node.test;

public class EngineToken {

	private String tag;
	private int tokenID;
	
	public EngineToken(String tag, int tokid) {
		this.tag = tag;
		tokenID = tokid;
	}
	
	public String getTag() {
		return tag;
	}
	
	public int getTokenID() {
		return tokenID;
	}
	
}
