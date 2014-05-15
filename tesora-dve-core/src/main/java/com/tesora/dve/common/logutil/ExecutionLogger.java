// OS_STATUS: public
package com.tesora.dve.common.logutil;


public interface ExecutionLogger {

	public void begin();
	public void end();

	public boolean completed();

	public long getDelta();

	public boolean isEnabled();
	
	public LogSubject getSubject();
	
	public ExecutionLogger getNewLogger(LogSubject subject);
	
	public ExecutionLogger getNewLogger(String s);
		
}
