// OS_STATUS: public
package com.tesora.dve.db.mysql;

public interface MysqlCommandFuture<T> {
	T sync();
}
