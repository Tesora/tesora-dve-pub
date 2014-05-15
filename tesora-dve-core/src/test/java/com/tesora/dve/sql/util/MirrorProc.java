// OS_STATUS: public
package com.tesora.dve.sql.util;

public abstract class MirrorProc extends MirrorTest {

	protected boolean dosys;
	protected boolean docheck;
	protected String description;
	
	public MirrorProc(String desc) {
		this(true,true, desc);
	}
	
	public MirrorProc() {
		this("mirrorproc");
	}
	
	public MirrorProc(boolean sys, boolean check, String desc) {
		dosys = sys;
		docheck = check;
		description = desc;
	}
	
	public abstract ResourceResponse execute(TestResource mr) throws Throwable;
	
	public String describe() {
		return description;
	}
	
	@Override
	public void execute(TestResource checkdb, TestResource sysdb) throws Throwable {
		ResourceResponse checkResponse = null;
		ResourceResponse sysResponse = null;
		if (docheck)
			checkResponse = execute(checkdb);
		if (dosys)
			sysResponse = execute(sysdb);
		if (checkResponse != null && sysResponse != null) 
			checkResponse.assertEqualResponse(describe(), sysResponse);
	}
}