// OS_STATUS: public
package com.tesora.dve.server.transactionmanager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.C3P0Registry;
import com.tesora.dve.common.catalog.TransactionRecord;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public class Transaction2PCTracker {
	
	private static Logger logger = Logger.getLogger(Transaction2PCTracker.class);

	enum TxnState { INIT, PREPARE, COMMIT, CLOSED };
	
	final TransactionRecord txnRec;
	
	TxnState txnState; 
	
	Future<Boolean> pendingOp;
	
	public Transaction2PCTracker(String transId) {
		this.txnRec = new TransactionRecord(transId);
		this.txnState = TxnState.INIT;
	}
	
	public void startPrepare() {
		if (txnState != TxnState.INIT)
			throw new PECodingException("Attempt to re-execute transaction prepare");

        pendingOp = Singletons.require(HostService.class).submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return txnRec.recordPrepare(getCatalogDS());
            }
        });
		
		txnState = TxnState.PREPARE;
	}
	
	public void startCommit() throws PEException {
		if (txnState != TxnState.PREPARE)
			throw new PECodingException("Attempt to execute 2PC commit without prepare");
		
		finishPrepare();

		if (!txnRec.recordCommit(getCatalogDS()))
			throw new PECodingException("Unable to execute 2PC commit");

		txnState = TxnState.COMMIT;
	}

	public void clearTransactionRecord() throws PEException {
		if (txnState == TxnState.PREPARE)
			finishPrepare();

        Singletons.require(HostService.class).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    txnRec.clearTransactionRecord(getCatalogDS());
                } catch (Throwable t) {
                    logger.warn("Exception cleaning up transaction record for transaction " + txnRec.getXid(), t);
                }
            }
        });

		txnState = TxnState.CLOSED;
	}

	public static void clearOldTransactionRecords() throws PEException {
        Singletons.require(HostService.class).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    TransactionRecord.clearOldTransactionRecords(getCatalogDS());
                } catch (Throwable t) {
                    logger.warn("Exception cleaning up old transaction records", t);
                }
            }
        });
	}

	private void finishPrepare() throws PEException {
		try {
			if (false == pendingOp.get())
				throw new PECodingException("Could not record prepare");
		} catch (InterruptedException e) {
			throw new PEException("Transaction Prepare Transcription was interrupted", e);
		} catch (ExecutionException e) {
			throw new PEException("Failure trying to record prepare of transaction", e);
		}
	}
	
	public static Map<String, Boolean> getGlobalCommitMap() throws PEException {
		return TransactionRecord.getGlobalCommitMap(getCatalogDS());
	}
	
	public static Map<String, Boolean> getCommitMapForHost(String host) throws PEException {
		return TransactionRecord.getCommitMapByHost(getCatalogDS(), host);
	}

	private static DataSource getCatalogDS() {
		@SuppressWarnings("unchecked")
		Set<DataSource> c3p0pools = C3P0Registry.allPooledDataSources(); 
		return c3p0pools.iterator().next();
	}
}
