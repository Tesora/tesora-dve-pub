// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.server.messaging.SQLCommand;

public class JustInTimeInsert {

	SQLCommand sql;
	long updateCount;
	DistributionKey key;
	
	public JustInTimeInsert(SQLCommand sql, long uc, DistributionKey key) {
		this.sql = sql;
		this.updateCount = uc;
		this.key = key;
	}
	
	public SQLCommand getSQL() {
		return sql;
	}
	
	public long getUpdateCount() {
		return updateCount;
	}
	
	public DistributionKey getKey() {
		return key;
	}
	
}
