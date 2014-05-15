// OS_STATUS: public
package com.tesora.dve.common.logutil;


public class AdHocSubject implements LogSubject {

	private final String description;
	
	public AdHocSubject(String s) {
		description = s;
	}
	
	@Override
	public String describeForLog() {
		return description;
	}

}
