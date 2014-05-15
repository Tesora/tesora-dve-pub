// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.tesora.dve.server.connectionmanager.log.ShutdownLog;
import org.apache.log4j.Logger;

import com.tesora.dve.common.RemoteException;
import com.tesora.dve.server.connectionmanager.UserNotification.NotificationType;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.server.statistics.RollingStatisticsAccumulator;
import com.tesora.dve.sql.schema.PEStorageSite.TCacheSite;
import com.tesora.dve.worker.agent.Agent;

public class NotificationManager extends Agent {
	
	private static final int PER_MINUTE_THRESHOLD = 10;

	public static int PER_SECOND_THRESHOLD = 1;

	private Logger logger = Logger.getLogger(NotificationManager.class);

	Map<StorageSite, RollingStatisticsAccumulator> failureStatMap1sec = new HashMap<StorageSite, RollingStatisticsAccumulator>();  
	Map<StorageSite, RollingStatisticsAccumulator> failureStatMap1min = new HashMap<StorageSite, RollingStatisticsAccumulator>();  

	public NotificationManager() throws PEException {
		super(NotificationManager.class.getSimpleName());
	}

	@Override
	public void onMessage(Envelope e) throws PEException {
		Object payload = e.getPayload();	
		if (payload instanceof NotificationManagerRequest) {
			NotificationManagerRequest m = (NotificationManagerRequest)payload;
			ResponseMessage resp;
			try {
				resp =  m.executeRequest(e, this);
			} catch (Exception ex) {
				resp = new GenericResponse().setException(new RemoteException(getName(), ex));
			}
			if (resp != null)
				returnResponse(e, resp);
		} else {
			throw new PEException("WorkerManager received message of invalid type (" + payload.toString() + ")");
		}
	}

	public synchronized void onSiteFailure(StorageSite site) throws PEException {
		notify(new UserNotification(NotificationType.SiteFailure, 
				"Communications failure on site " + site + " (url: " + site.getMasterUrl() + ")"));
		if (false == failureStatMap1sec.containsKey(site)) {
			failureStatMap1sec.put(site, new RollingStatisticsAccumulator(10, 100));
			failureStatMap1min.put(site, new RollingStatisticsAccumulator(10, 6000));
		}
		failureStatMap1sec.get(site).addDatum(1);
		failureStatMap1min.get(site).addDatum(1);
		if (failureStatMap1sec.get(site).getTransactionsPerSecond() >= PER_SECOND_THRESHOLD || failureStatMap1min.get(site).getTransactionsPerSecond() >= PER_MINUTE_THRESHOLD) {
			CatalogDAO c = CatalogDAOFactory.newInstance();
			try {
				StorageSite realSite = site instanceof TCacheSite ? site.getRecoverableSite(c) : site;
				if (realSite.getMasterUrl().equals(site.getMasterUrl()))
					site.onSiteFailure(c);
			} finally {
				c.close();
			}
		}
	}

	private void notify(UserNotification userNotification) {
		logger.warn(userNotification.getNotificationMessage());
	}

	@Override
	public synchronized void onTimeout() {
		for (Iterator<Entry<StorageSite, RollingStatisticsAccumulator>> i = failureStatMap1min.entrySet().iterator(); i.hasNext();) {
			Entry<StorageSite, RollingStatisticsAccumulator> entry = i.next();
			if (failureStatMap1min.get(entry.getKey()).getTransactionsPerSecond() == 0) {
				i.remove();
				failureStatMap1sec.remove(entry.getKey());
			}
		}
	}

	public void shutdown() {
		try {
			super.close();
		} catch (PEException e) {
			ShutdownLog.logShutdownError("Error shutting down " + getClass().getSimpleName(), e);
		}
	}

}
