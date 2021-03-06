package com.tesora.dve.server.messaging;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.DevXid;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.Worker;

public class WorkerRecoverSiteRequest extends WorkerRequest {

	static Logger logger = Logger.getLogger( WorkerRecoverSiteRequest.class );

	private static final long serialVersionUID = 1L;
	
	final Map<String, Boolean> commitMap;
	
	public WorkerRecoverSiteRequest(SSContext ctx, Map<String, Boolean> commitMap) {
		super(ctx);
		this.commitMap = commitMap;
	}

	@Override
	public void executeRequest(final Worker w, final CompletionHandle<Boolean> callersResults) {
        try {
            final MysqlTextResultCollector results = new MysqlTextResultCollector();

            PEDefaultPromise<Boolean> recoverListSQL = new PEDefaultPromise<Boolean>(){
                @Override
                public void success(Boolean xaRecoverHadResults) {
                    if (xaRecoverHadResults){
                        List<Pair<String, Boolean>> xidsToRecover = buildRecoverList(w, results);
                        final Iterator<Pair<String, Boolean>> recoveryItemIterator = xidsToRecover.iterator();

                        recoverNextItem(w, recoveryItemIterator, callersResults);
                    } else {
                        Exception codingError = new PECodingException("XA RECOVER did not return results");
                        codingError.fillInStackTrace();
                        this.failure(codingError);
                    }
                }

                @Override
                public void failure(Exception t) {
                    super.failure(t);
                }
            };
            //TODO: ignores provided groupDispatch
            this.withGroupDispatch(results);
            this.execute(w, new SQLCommand(PerHostConnectionManager.INSTANCE.lookupConnection(this.getConnectionId()),"XA RECOVER"), recoverListSQL);
        } catch (Exception e) {
            callersResults.failure(e);
        }
	}

    private void recoverNextItem(final Worker w, final Iterator<Pair<String, Boolean>> recoveryItemIterator, final CompletionHandle<Boolean> callersPromise) {
        if (!recoveryItemIterator.hasNext()){
            logger.info(w.getName() + ": Completed XA Transaction Recovery for site " + w.getWorkerSite());
            callersPromise.success(true);
            return; //finished.
        }

        Pair<String, Boolean> recoveryInfo = recoveryItemIterator.next();
        String recoverStatement;
        String xid = recoveryInfo.getFirst();
        Boolean hasCommitted = recoveryInfo.getSecond();

        CompletionHandle<Boolean> resultForCurrentItem = new PEDefaultPromise<Boolean>(){
            @Override
            public void success(Boolean returnValue) {
                recoverNextItem(w, recoveryItemIterator, callersPromise);
            }

            @Override
            public void failure(Exception t) {
                callersPromise.failure(t);
            }
        };

        if (hasCommitted)
            recoverStatement = "XA COMMIT " + xid;
        else
            recoverStatement = "XA ROLLBACK " + xid;
        //TODO: ignores provided groupDispatch
        this.withGroupDispatch(DBEmptyTextResultConsumer.INSTANCE);
        this.execute(w, new SQLCommand(PerHostConnectionManager.INSTANCE.lookupConnection(this.getConnectionId()),recoverStatement), resultForCurrentItem);
    }

    private List<Pair<String, Boolean>> buildRecoverList(Worker w, MysqlTextResultCollector results) {
        List<Pair<String, Boolean>> xidsToRecover;
        xidsToRecover = new ArrayList<Pair<String,Boolean>>();

        logger.info(w.getName() + ": Beginning XA Transaction Recovery for site " + w.getWorkerSite());
        //returned columns are {formatID,gtrid_length,bqual_length,data}
        for (List<String> row : results.getRowData()) {
            int formatId = Integer.parseInt(row.get(0));
int gtrid_length = Integer.parseInt(row.get(1));
//bqual_length = Integer.parseInt(row.get(1));
            String xidString = row.get(3);

            if (formatId == DevXid.FORMAT_ID) {
                String gtrid = xidString.substring(0, gtrid_length);
                String bqual = xidString.substring(gtrid_length);
                String recoveryXid = "'" + gtrid + "','" + bqual + "'," + DevXid.FORMAT_ID;

                if (commitMap.containsKey(gtrid))
                    xidsToRecover.add(new Pair<String, Boolean>(recoveryXid, commitMap.get(gtrid)));
            }
            else {
                if (logger.isDebugEnabled())
                    logger.debug("Skipped recoverable transaction with format id " + formatId);
            }
        }
        return xidsToRecover;
    }

    @Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return null;
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		return null;
	}

}
