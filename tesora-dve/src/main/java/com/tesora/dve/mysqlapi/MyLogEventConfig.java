// OS_STATUS: public
package com.tesora.dve.mysqlapi;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class MyLogEventConfig {
	String logMsgFile = null;
	String endLogFile = null;
	Long endLogPos = null;
	Set<String> dbFilter = new HashSet<String>();
	CountDownLatch latch;
	
	public MyLogEventConfig(CountDownLatch latch) {
		this.latch = latch;
	}
	
	public String getLogMsgFile() {
		return logMsgFile;
	}
	public void setLogMsgFile(String logMsgFile) {
		this.logMsgFile = logMsgFile;
	}
	public String getEndLogFile() {
		return endLogFile;
	}
	public void setEndLogFile(String endLogFile) {
		this.endLogFile = endLogFile;
	}
	public Long getEndLogPos() {
		return endLogPos;
	}
	public void setEndLogPos(Long endLogPos) {
		this.endLogPos = endLogPos;
	}
	public Set<String> getDbFilter() {
		return dbFilter;
	}
	public void setDbFilter(Set<String> dbFilter) {
		this.dbFilter = dbFilter;
	}
	
	public CountDownLatch getLatch() {
		return latch;
	}



}
