// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.concurrent.PEPromise;

public abstract class MysqlConcurrentCommand extends MysqlCommand {

	private PEPromise<Boolean> promise;

	public MysqlConcurrentCommand(PEPromise<Boolean> promise) {
		this.promise = promise;
	}

	public PEPromise<Boolean> getPromise() {
		return promise;
	}
}
