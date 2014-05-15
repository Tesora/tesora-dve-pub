// OS_STATUS: public
package com.tesora.dve.common.logutil;


public class DisabledExecutionLogger implements ExecutionLogger {

	public static final DisabledExecutionLogger defaultDisabled = new DisabledExecutionLogger();
	
	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void end() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean completed() {
		return false;
	}

	@Override
	public long getDelta() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isEnabled() {
		return false;
	}

	@Override
	public LogSubject getSubject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExecutionLogger getNewLogger(LogSubject subject) {
		return defaultDisabled;
	}

	@Override
	public ExecutionLogger getNewLogger(String s) {
		return defaultDisabled;
	}
}
