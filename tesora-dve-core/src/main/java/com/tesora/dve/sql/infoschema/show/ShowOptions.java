// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

public class ShowOptions {

	private boolean full = false;
	
	public ShowOptions() {
		
	}
	
	public boolean isFull() {
		return full;
	}
	
	public ShowOptions setFull() {
		full = true;
		return this;
	}
	
}
