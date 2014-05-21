// OS_STATUS: public
package com.tesora.dve.server.bootstrap;

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

import java.lang.management.ManagementFactory;
import java.util.Enumeration;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.tesora.dve.server.connectionmanager.BroadcastMessageAgent;
import com.tesora.dve.server.connectionmanager.NotificationManager;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.server.global.BootstrapHostService;
import com.tesora.dve.server.global.MySqlPortalService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.log4j.spi.LoggerRepository;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.comms.client.messages.ConnectRequest;
import com.tesora.dve.comms.client.messages.GlobalRecoveryRequest;
import com.tesora.dve.db.mysql.portal.MySqlPortal;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.server.statistics.manager.StatisticsManager;
import com.tesora.dve.server.transactionmanager.Transaction2PCTracker;
import com.tesora.dve.siteprovider.SiteProviderContextInitialisation;
import com.tesora.dve.siteprovider.SiteProviderPlugin.SiteProviderContext;
import com.tesora.dve.siteprovider.SiteProviderPlugin.SiteProviderFactory;
import com.tesora.dve.smqplugin.SimpleMQPlugin;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.schema.mt.TableGarbageCollector;
import com.tesora.dve.variable.GlobalConfigVariableHandler;
import com.tesora.dve.worker.agent.Agent;
import com.tesora.dve.worker.SingleDirectConnection;
import com.tesora.dve.worker.WorkerManager;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class BootstrapHost extends Host implements BootstrapHostMBean, BootstrapHostService {

	private static final String MBEAN_BOOTSTRAP_HOST = "com.tesora.dve:name=Server";
	private static final String MBEAN_LOG4J_HIERARCHY = "log4j:hierarchy=default";

	static {
		logger = Logger.getLogger(BootstrapHost.class);
	}

    WorkerManager workerManager;
	BroadcastMessageAgent broadcastMessageAgent;
	Transaction2PCTracker transactionManager;
	StatisticsManager statisticsManager;
	NotificationManager notificationManager;
	int statisticsPersisterInterval;
	TableGarbageCollector tgc;
	int tableCleanupInterval;

	public BootstrapHost(String name, Properties props) {
		super(name, props);
        Singletons.replace(BootstrapHostService.class,this);

		try {
			workerManager = new WorkerManager();
			setWorkerManagerAddress(workerManager.getAddress());

			notificationManager = new NotificationManager();
			setNotificationManagerAddress(notificationManager.getAddress());

			statisticsManager = new StatisticsManager();
			setStatisticsManagerAddress(statisticsManager.getAddress());

			broadcastMessageAgent = new BroadcastMessageAgent();
			setBroadcastMessageAgentAddress(broadcastMessageAgent.getAddress());

			WorkerGroupFactory.startup(workerManager);

			initializeCatalogServices();

			if (GroupManager.getCoordinationServices().localMemberIsOldestMember())
				doGlobalRecovery();
			
			registerMBeans();

		} catch (PEException e) {
			throw new RuntimeException("Failed to start DVE server - " + e.rootCause().getMessage(), e);
		} catch (Throwable e) {
			throw new RuntimeException("Failed to start DVE server - " + e.getMessage(), e);
		}
	}
	
	public WorkerManager getWorkerManager() {
		return workerManager;
	}

	private void doGlobalRecovery() throws PEException {
		User rootUser = getProject().getRootUser();
		
		SSConnectionProxy ssConProxy = new SSConnectionProxy();
		try {
			ssConProxy.executeRequest(new ConnectRequest(rootUser.getName(), rootUser.getPlaintextPassword()));
			ssConProxy.executeRequest(new GlobalRecoveryRequest());
		} finally {
			ssConProxy.close();
		}
	}
	
	@Override
	protected void registerMBeans() {
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();

		try {
			server.registerMBean(this, new ObjectName(MBEAN_BOOTSTRAP_HOST));
		} catch(Exception e) {
			logger.error("Unable to register " + MBEAN_BOOTSTRAP_HOST + " mbean" , e);
		}

		try {
			HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();
            server.registerMBean(hdm, new ObjectName(MBEAN_LOG4J_HIERARCHY));

            // Add the root logger to the Hierarchy MBean
            hdm.addLoggerMBean(Logger.getRootLogger().getName());

            LoggerRepository r = LogManager.getLoggerRepository();

            @SuppressWarnings("rawtypes")
			Enumeration loggers = r.getCurrentLoggers();
            while ( loggers.hasMoreElements() ) {
                hdm.addLoggerMBean(((Logger)loggers.nextElement()).getName());
            }
		}
		catch(Exception e) {
			logger.error("Unable to register " + MBEAN_LOG4J_HIERARCHY + " mbean" , e);
		}

		super.registerMBeans();
	}
	
	@Override
	protected void unregisterMBeans() {
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();

		try {
			server.unregisterMBean(new ObjectName(MBEAN_BOOTSTRAP_HOST));
		} catch (Exception e) {
			logger.error("Unable to unregister " + MBEAN_BOOTSTRAP_HOST + " mBean", e);
		}

		try {
			server.unregisterMBean(new ObjectName(MBEAN_LOG4J_HIERARCHY));
		} catch (Exception e) {
			logger.error("Unable to unregister " + MBEAN_LOG4J_HIERARCHY + " mBean", e);
		}
		
		super.unregisterMBeans();
	}

	public static <T> BootstrapHost startServices(Class<T> bootstrapClass) throws Exception {
		return startServices(bootstrapClass, PEFileUtils.loadPropertiesFile(bootstrapClass, PEConstants.CONFIG_FILE_NAME));
	}
	
	public static <T> BootstrapHost startServices(Class<T> bootstrapClass, Properties props) throws Exception {
		
		String url = props.getProperty(DBHelper.CONN_URL);
		String database = props.getProperty(DBHelper.CONN_DBNAME, PEConstants.CATALOG);

		// Fail to start if we don't have a valid catalog connection URL
		if(StringUtils.isBlank(url))
			throw new PEException("Value for " + DBHelper.CONN_URL + " not specified in server configuration");

		if(StringUtils.isBlank(database))
			throw new PEException("Value for " + DBHelper.CONN_DBNAME + " not specified in server configuration");

		// Attempt to load the JDBC driver - fail early if it isn't available
		DBHelper.loadDriver(props.getProperty(DBHelper.CONN_DRIVER_CLASS));

		props.put(DBHelper.CONN_DBNAME,  database);
		DBHelper helper = new DBHelper(props);
		try {
			// Check that we can connect to the database and that the catalog database exists
			helper.connect();

			// and also check that it is a valid catalog
			helper.executeQuery("SELECT name FROM " + database + ".project");
		} catch(Exception e) {
			throw new Exception("A DVE catalog couldn't be found at '" + url + "' with name '" + database + "'");
		} finally  {
			helper.disconnect();
		}
			
		// Temporarily set the log level to info so that the below message will print and force the 
		// header to print
		Level logLevel = logger.getLevel();		// this is needed in case an explicit level is set on this category
		Level effectiveLevel = logger.getEffectiveLevel();	// if an explicit level isn't set, this gets the level from the hierarchy
		logger.setLevel(Level.INFO);
		logger.info("Starting DVE server using:");
		logger.info("... Catalog URL      : " + url);
		logger.info("... Catalog database : " + database);
		logger.info("... Catalog User     : " + props.getProperty(DBHelper.CONN_USER));
		logger.info("Resetting log level back to: " + effectiveLevel);
		logger.setLevel(logLevel);

		// We have to initialize the Group Manager before anything else.
		GroupManager.initialize(props);
		GroupManager.getCoordinationServices().registerWithGroup(props);

		CatalogDAOFactory.setup(props);
		
		SchemaSourceFactory.reset();

		Agent.setPluginProvider(SimpleMQPlugin.class);
		Agent.startServices(props);

		BootstrapHost host = new BootstrapHost(bootstrapClass.getName(), props);

        ClusterLock genLock = GroupManager.getCoordinationServices().getClusterLock(RangeDistributionModel.GENERATION_LOCKNAME);
        String reason = "boostrapping system";
        genLock.sharedLock(null, reason);
		genLock.sharedUnlock(null,reason);
		
		MySqlPortal.start(props);

		return host;
	}

	public static void stopServices() throws Exception {
		MySqlPortal.stop();

		if (GroupManager.getCoordinationServices() != null)
			GroupManager.getCoordinationServices().unRegisterWithGroup();

        Host.hostStop();

		Agent.stopServices();
		CatalogDAOFactory.shutdown();
		AutoIncrementTracker.clearCache();
		RangeDistributionModel.clearCache();
		RandomDistributionModel.clearCache();
		SiteProviderFactory.closeSiteProviders();
		ExternalServiceFactory.closeExternalServices();
		GroupManager.shutdown();
		SingleDirectConnection.clearConnectionCache();
	}

    @Override
	protected void stop() {
		stopTableCleanup();

		super.stop();
	}

	@Override
    protected void close() {
		unregisterMBeans();

		super.close();

		// No shutdown methods should throw exceptions as we could do nothing but print it
		notificationManager.shutdown();
		WorkerGroupFactory.shutdown(workerManager);
		workerManager.shutdown();
		statisticsManager.shutdown();
		broadcastMessageAgent.shutdown();
	}

	private void initializeCatalogServices() throws Exception {
		CatalogDAO c = CatalogDAOFactory.newInstance();
		try {
			c.recoverTransactions();

			// Create and configure all Dynamic Site Providers
			for (Provider provider : c.findAllProviders()) {
				SiteProviderContext ctxt = new SiteProviderContextInitialisation(provider.getName(), c);
				SiteProviderFactory.addProvider(ctxt, provider.getPlugin(), provider.getName(), provider.isEnabled(),
						provider.getConfig());
			}

			// Create and configure External Services
			for (ExternalService es : c.findAllExternalServices()) {
				// enclose in try/catch in case an early service fails we still
				// start the later ones
				try {
					ExternalServiceFactory.register(es.getName(), es.getPlugin());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			GlobalConfigVariableHandler.initializeGlobalVariables(getGlobalVariables(c));
		} finally {
			c.close();
		}
	}

	// -------------------------------------------------------------------------

	public void startTableCleanup() {
		if (tableCleanupInterval > 0) {
			tgc = new TableGarbageCollector(tableCleanupInterval);
			tgc.start();
		}
	}

	private void stopTableCleanup() {
		if (tgc != null) {
			tgc.interrupt();
			try {
				tgc.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void setTableCleanupInterval(int value, boolean action) {
		int oldValue = tableCleanupInterval;
		tableCleanupInterval = value;

		if (action) {
			if (oldValue == 0)
				startTableCleanup();
			else if (tableCleanupInterval == 0)
				stopTableCleanup();
		}
	}

	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------
	// The following methods are for the BootstrapHost MBean

	@Override
	public int getPortalWorkerGroupCount() {
        MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
        return portal == null ? 0 : portal.getWorkerGroupCount();
	}
		
	@Override
	public int getPortalClientExecutorActiveCount() {
        MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
        return portal == null ? 0 : portal.getClientExecutorActiveCount();
	}
		
	@Override
	public int getPortalClientExecutorPoolSize() {
        MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
        return portal == null ? 0 : portal.getClientExecutorPoolSize();
	}
		
	@Override
	public int getPortalClientExecutorLargestPoolSize() {
        MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
        return portal == null ? 0 : portal.getClientExecutorLargestPoolSize();
	}
		
	@Override
	public int getPortalClientExecutorMaximumPoolSize() {
        MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
        return portal == null ? 0 : portal.getClientExecutorMaximumPoolSize();
	}

	@Override
	public int getPortalClientExecutorQueueSize() {
        MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
        return portal == null ? 0 : portal.getClientExecutorQueueSize();
	}
		
	@Override
	public void onGarbageEvent() {
		if (tgc != null) tgc.onGarbageEvent();
	}
	
}
