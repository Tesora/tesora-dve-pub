package com.tesora.dve.sql.logfile;

public interface LogFileRunner {

	public void runFile() throws Throwable;
	
	public String getDelayedFailures() throws Throwable;

	public void close() throws Throwable;
	
}
