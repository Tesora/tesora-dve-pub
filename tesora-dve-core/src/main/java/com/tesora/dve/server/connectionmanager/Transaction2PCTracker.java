package com.tesora.dve.server.connectionmanager;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
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
                return txnRec.recordPrepare(CatalogDAO.getCatalogDS());
            }
        });
		
		txnState = TxnState.PREPARE;
	}
	
	public void startCommit() throws PEException {
		if (txnState != TxnState.PREPARE)
			throw new PECodingException("Attempt to execute 2PC commit without prepare");
		
		finishPrepare();

		if (!txnRec.recordCommit(CatalogDAO.getCatalogDS()))
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
                    txnRec.clearTransactionRecord(CatalogDAO.getCatalogDS());
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
                    TransactionRecord.clearOldTransactionRecords(CatalogDAO.getCatalogDS());
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
		return TransactionRecord.getGlobalCommitMap(CatalogDAO.getCatalogDS());
	}
	
	public static Map<String, Boolean> getCommitMapForHost(String host) throws PEException {
		return TransactionRecord.getCommitMapByHost(CatalogDAO.getCatalogDS(), host);
	}
}
