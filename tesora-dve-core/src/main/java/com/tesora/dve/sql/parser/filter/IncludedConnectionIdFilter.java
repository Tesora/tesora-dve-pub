// OS_STATUS: public
package com.tesora.dve.sql.parser.filter;

import java.util.Set;

public class IncludedConnectionIdFilter implements LogFileFilter {
	Set<Integer> connIds;
	
	public IncludedConnectionIdFilter(Set<Integer> connIds) {
		this.connIds = connIds;
	}

	@Override
	public boolean filterLine(int connectionId, String statement) {
		return !connIds.contains(connectionId);
	}

}
