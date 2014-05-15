// OS_STATUS: public
package com.tesora.dve.db;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.DevXid;

public interface DBConnection extends PEFuture.Listener<Boolean> {
	
	interface Factory {
		DBConnection newInstance(StorageSite site);
	}
	
	interface Monitor {
		void onUpdate();
	}
	
	void connect(String url, String userid, String password) throws PEException;
	void close();
	
	PEFuture<Boolean> execute(SQLCommand sql, DBResultConsumer consumer, PEPromise<Boolean> promise) throws PESQLException;
	void execute(SQLCommand sql, DBResultConsumer consumer) throws PESQLException;
	
	void start(DevXid xid) throws Exception;
	void end(DevXid xid) throws Exception;
	void prepare(DevXid xid) throws Exception;
	void commit(DevXid xid, boolean onePhase) throws Exception;
	void rollback(DevXid xid) throws Exception;
	void setCatalog(String databaseName) throws Exception;
	void cancel();

	boolean hasPendingUpdate();
	boolean hasActiveTransaction();
	int getConnectionId();
}
