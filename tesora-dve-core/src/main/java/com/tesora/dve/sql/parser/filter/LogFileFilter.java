// OS_STATUS: public
package com.tesora.dve.sql.parser.filter;

public interface LogFileFilter {

	public boolean filterLine(int connectionId, String statement);
	
}
