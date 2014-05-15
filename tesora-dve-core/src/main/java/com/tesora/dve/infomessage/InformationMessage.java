// OS_STATUS: public
package com.tesora.dve.infomessage;

public class InformationMessage {

	private final Level level;
	private final String message;
	private final int code;
	
	public InformationMessage(Level l, String m) {
		level = l;
		message = m;
		code = 9999;
	}
	
	public boolean atLeast(Level l) {
		return level.ordinal() >= l.ordinal();
	}
	
	public boolean only(Level l) {
		return level == l;
	}
	
	public int getCode() {
		return code;
	}
	
	public String getMessage() {
		return message;
	}	
	
	public Level getLevel() {
		return level;
	}
}
