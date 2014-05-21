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
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.management.DynamicMBean;
import javax.management.ObjectName;

import com.tesora.dve.clock.*;
import com.tesora.dve.groupmanager.GroupTopicPublisher;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.BootstrapHostService;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.tesora.dve.charset.CharSetNative;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PELogUtils;
import com.tesora.dve.common.catalog.AutoIncrementTrackerDynamicMBean;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.concurrent.NamedTaskExecutorService;
import com.tesora.dve.concurrent.PEThreadPoolExecutor;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.groupmanager.GroupMessageEndpoint;
import com.tesora.dve.groupmanager.OnGlobalConfigChangeMessage;
import com.tesora.dve.sql.infoschema.InformationSchemas;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.upgrade.CatalogVersions;
import com.tesora.dve.variable.ConfigVariableHandler;
import com.tesora.dve.variable.ConfigVariableHandlerDynamicMBean;
import com.tesora.dve.variable.GlobalVariableHandler;
import com.tesora.dve.variable.ScopedVariableHandler;
import com.tesora.dve.variable.SessionVariableHandler;
import com.tesora.dve.variable.VariableConfig;
import com.tesora.dve.variable.VariableHandler;
import com.tesora.dve.variable.VariableInfo;
import com.tesora.dve.variable.VariableValueStore;
import com.tesora.dve.variable.status.StatusVariableHandler;
import com.tesora.dve.variable.status.StatusVariableHandlerDynamicMBean;

public class Host implements HostService {
    protected static Logger logger = Logger.getLogger(Host.class);

	private static final String MBEAN_GLOBAL_CONFIG = "com.tesora.dve:name=ConfigVariables";
	private static final String MBEAN_GLOBAL_STATUS = "com.tesora.dve:name=StatusVariables";
	private static final String MBEAN_AUTO_INCR  =  "com.tesora.dve:name=AutoIncrementTracker";

	private static final String CONFIG_VARIABLES_XML = "configVariables.xml";
	private static final String TIMEOUT_KEY = "worker.timeout";
	private static final String TIMEOUT_DEFAULT = "10";
	
	private String versionComment = null;
	
	static Host singleton = null;

	private NamedTaskExecutorService executorService;

	private int dbConnectionTimeout = -1;

	private String name;
	private String hostName;
	private String workerManagerAddress;
	private String statisticsManagerAddress;
	private String transactionManagerAddress;
	private String broadcastMessageAgentAddress;
	private String notificationManagerAddress;

    protected static void hostStop() {
        if(singleton != null) {
           singleton.stop();
           singleton.close();
        }
    }

    @Override
    public String getBroadcastMessageAgentAddress() {
		return broadcastMessageAgentAddress;
	}

	@Override
    public void setBroadcastMessageAgentAddress(String broadcastMessageAgentAddress) {
		this.broadcastMessageAgentAddress = broadcastMessageAgentAddress;
	}

	@Override
    public String getNotificationManagerAddress() {
		return notificationManagerAddress;
	}

	@Override
    public void setNotificationManagerAddress(String notificationManagerAddress) {
		this.notificationManagerAddress = notificationManagerAddress;
	}	
	
	protected void setTransactionManagerAddress(String address) {
		transactionManagerAddress = address;
	}

	@Override
    public String getTransactionManagerAddress() {
		return transactionManagerAddress;
	}

	@Override
    public String getStatisticsManagerAddress() {
		return statisticsManagerAddress;
	}

	@Override
    public void setStatisticsManagerAddress(String statisticsManagerAddress) {
		this.statisticsManagerAddress = statisticsManagerAddress;
	}
	
	@Override
    public String getWorkerManagerAddress() {
		return workerManagerAddress;
	}
	
	protected void setWorkerManagerAddress(String addr) {
		workerManagerAddress = addr;
	}

	private Long startTime;
	
	private Project project;
	private DBNative dbNative;
	private CharSetNative charSetNative;
	
	private Properties props;

	private VariableConfig<GlobalVariableHandler> globalConfig;
	private ConfigVariableHandlerDynamicMBean globalConfigMBean = new ConfigVariableHandlerDynamicMBean();

	private VariableConfig<SessionVariableHandler> sessionConfigTemplate;
	private VariableValueStore sessionConfigDefaults;

	private Map<String /* providerName */, VariableConfig<ScopedVariableHandler>> scopedVariables 
		= new HashMap<String, VariableConfig<ScopedVariableHandler>>();

	private VariableConfig<StatusVariableHandler> statusVariables;
	private StatusVariableHandlerDynamicMBean globalStatusMBean = new StatusVariableHandlerDynamicMBean();
	private DynamicMBean autoIncrementTrackerMBean = new AutoIncrementTrackerDynamicMBean();

	
	private InformationSchemas infoSchema;


	private GroupMessageEndpoint broadcaster;

	private TimeBasedGenerator uuidGenerator = null;
	private String dveVersion = null;


	public Host(String name, Properties props) {
		this.name = name;
		this.props = props;
		
		singleton = this;
        registerTimingService();
        Singletons.replace(HostService.class, singleton); 
        if (this instanceof BootstrapHost){
            //dirty, but works around issue where initializers were accessing Bootstrap singleton before this constructor exited and bootstrap constructor had time to register sub-interface. -sgossard
            Singletons.replace(BootstrapHostService.class, (BootstrapHost)singleton);
        }

        this.executorService = new PEThreadPoolExecutor("Host");

		try {
			hostName = InetAddress.getLocalHost().getHostName();
			
			if (name == null) {
				name = hostName;
			}
			
			CatalogVersions.catalogVersionCheck(props);
			
			DBType dbType = DBType.fromDriverClass(props.getProperty(DBHelper.CONN_DRIVER_CLASS));

			dbNative = DBNative.DBNativeFactory.newInstance(dbType);
			charSetNative = CharSetNative.CharSetNativeFactory.newInstance(dbType);

			CatalogDAO c = CatalogDAOFactory.newInstance();
			try {
				project = c.findProject(props.getProperty("defaultproject", Project.DEFAULT));
				
				initialiseConfigVariables(c);
				initialiseSessionVariableTemplate(false);
				initialiseStatusVariables();
			} finally {
				c.close();
			}
			
			infoSchema = InformationSchemas.build(dbNative);

			broadcaster = new GroupMessageEndpoint("broadcast");
            Singletons.replace(GroupTopicPublisher.class, broadcaster);

			startTime = System.currentTimeMillis();
			
		} catch (Throwable e) {
            throw new RuntimeException("Failed to start DVE server - " + e.getMessage(), e);
		}
	}

	/**
	 * This constructor is to facilitate the TestHost which is used only 
	 * for testing 
	 * 
	 * @param props - properties file containing the javax.persistence.jdbc.driver
	 * 				  property
	 * @param startCatalog - should the CatalogDAOFactory be called to be available to 
	 * 						 connect to the catalog. Note: if true, the props must contain
	 * 						 the relevant parameters to connect to it.
	 */
	public Host(Properties props, boolean startCatalog) {
		this.props = props;
        registerTimingService();

        this.executorService = new PEThreadPoolExecutor("Host");

		try {
			hostName = InetAddress.getLocalHost().getHostName();

			if ( startCatalog )
				CatalogDAOFactory.setup(props);

			String driver = DBHelper.loadDriver(props.getProperty(DBHelper.CONN_DRIVER_CLASS));
			
			DBType dbType = DBType.fromDriverClass(driver);
			dbNative = DBNative.DBNativeFactory.newInstance(dbType);
			charSetNative = CharSetNative.CharSetNativeFactory.newInstance(dbType, startCatalog);
			
			infoSchema = InformationSchemas.build(dbNative);
			initialiseSessionVariableTemplate(true);
		} catch (Exception e) {
            throw new RuntimeException("Failed to start DVE server - " + e.getMessage(), e);
		}
		singleton = this;
        Singletons.replace(HostService.class, singleton);
	}

    public void registerTimingService() {
        Path baseLogDir = null;
        try {
            baseLogDir = Paths.get( System.getProperty("tesora.dve.log") );
        } catch (Exception e){
            logger.warn("Problem getting log dir from the 'tesora.dve.log' system property, logging may not work properly.");
        }

        SwitchingTimingService concreteImpl = new SwitchingTimingService(baseLogDir);
        Singletons.replace(TimingService.class, concreteImpl);
        Singletons.replace(TimingServiceConfiguration.class, concreteImpl);
    }

    @SuppressWarnings("unchecked")
	private void initialiseStatusVariables() throws PEException {
		statusVariables = VariableConfig.parseXML(dbNative.getStatusVariableConfigName());

		Method ctor;
		for (VariableInfo<StatusVariableHandler> vInfo : statusVariables.getInfoValues()) {
			StatusVariableHandler vHandler = vInfo.getHandler();
			
			if (vHandler == null)
				throw new PENotFoundException("Handler for Status variable '" + vInfo.getName() + "' not found");
			
			// we initialise variables with handlers that define initialise(String, String).
			try {
				ctor = vHandler.getClass().getMethod("initialise", new Class[] {String.class, String.class});
				ctor.invoke(vHandler, vInfo.getName(), vInfo.getDefaultValue());
			} catch (NoSuchMethodException e1) {
				// this is OK, it just means that the handler hasn't supplied an initialise method we know
			} catch (Exception e2) {
				throw new PEException("Exception looking up initialiser for Status variable '" + vInfo.getName() + "'", e2);
			}
			
			globalStatusMBean.add(vInfo.getName(), (StatusVariableHandler)vHandler);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void initialiseSessionVariableTemplate(boolean sessionOnly) throws PEException {
		Method ctor;
		sessionConfigTemplate = VariableConfig.parseXML(dbNative.getSessionVariableConfigName());

		for (VariableInfo<SessionVariableHandler> vInfo : sessionConfigTemplate.getInfoValues()) {
			VariableHandler vHandler = vInfo.getHandler();
			
			if (vHandler == null)
				throw new PENotFoundException("Handler for Session variable '" + vInfo.getName() + "' not found");

			if (sessionOnly && ((SessionVariableHandler)vHandler).defaultValueRequiresConnection())
				continue;
			
			// we initialise variables with handlers that define initialise(String, String).
			try {
				ctor = vHandler.getClass().getMethod("initialise", new Class[] {String.class, String.class});
				ctor.invoke(vHandler, vInfo.getName(), vInfo.getDefaultValue());
			} catch (NoSuchMethodException e1) {
				// this is OK, it just means that the handler hasn't supplied an initialise method we know
				// TODO: what if there is an initialise method with different parameters?
			} catch (Exception e2) {
				throw new PEException("Exception looking up initialiser for Config variable '" + vInfo.getName() + "'", e2);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void initialiseConfigVariables(CatalogDAO c) throws PEException {
		Method ctor;

		globalConfig = VariableConfig.parseXML(CONFIG_VARIABLES_XML);
		for (VariableInfo<GlobalVariableHandler> vInfo : globalConfig.getInfoValues()) {
			VariableHandler vHandler = vInfo.getHandler();

			// we initialise variables with handlers that define either initialise(CatalogDAO, String, String)
			// or just initialise(String, String).
			try {
				ctor = vHandler.getClass().getMethod("initialise", new Class[] {CatalogDAO.class, String.class, String.class});
				ctor.invoke(vHandler, c, vInfo.getName(), vInfo.getDefaultValue());
			} catch (NoSuchMethodException e) {
				// Constructor with CatalogDAO not found so try the other one
				
				try {
					ctor = vHandler.getClass().getMethod("initialise", new Class[] {String.class, String.class});
					ctor.invoke(vHandler, vInfo.getName(), vInfo.getDefaultValue());
				} catch (NoSuchMethodException e1) {
					// this is OK, it just means that the handler hasn't supplied an initialise method we know
					// TODO: what if there is an initialise method with different parameters?
				} catch (Exception e2) {
					throw new PEException("Exception looking up initialiser for " + vInfo.getName(), e2);
				}
			} catch (Exception e) {
				throw new PEException("Exception looking up initialiser for " + vInfo.getName(), e);
			}
			
			if (vHandler instanceof ConfigVariableHandler)
				globalConfigMBean.add(vInfo.getName(), (ConfigVariableHandler)vHandler);
		}

	}

    protected void stop() {
		// do nothing?
	}
	
	protected void close() {
	
	}
	
	protected void registerMBeans() {
		try {
			ManagementFactory.getPlatformMBeanServer().registerMBean(globalConfigMBean, new ObjectName(MBEAN_GLOBAL_CONFIG));
		} catch (Throwable e) {
			logger.error("Unable to register " + MBEAN_GLOBAL_CONFIG + " mbean" , e);
		}

		try {
			ManagementFactory.getPlatformMBeanServer().registerMBean(globalStatusMBean, new ObjectName(MBEAN_GLOBAL_STATUS));
		} catch (Throwable e) {
			logger.error("Unable to register " + MBEAN_GLOBAL_STATUS + " mbean" , e);
		}
		
		try {
			ManagementFactory.getPlatformMBeanServer().registerMBean(autoIncrementTrackerMBean, new ObjectName(MBEAN_AUTO_INCR));
		} catch (Throwable e) {
			logger.error("Unable to register " + MBEAN_AUTO_INCR + " mbean" , e);
		}

	}
	
	protected void unregisterMBeans() {
		try {
			ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(MBEAN_GLOBAL_STATUS));
		} catch (Exception e) {
			logger.error("Unable to unregister " + MBEAN_GLOBAL_STATUS + " mBean", e);
		}

		try {
			ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(MBEAN_GLOBAL_CONFIG));
		} catch (Exception e) {
			logger.error("Unable to unregister " + MBEAN_GLOBAL_CONFIG + " mBean", e);
		}
		
		try {
			ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(MBEAN_AUTO_INCR));
		} catch (Exception e) {
			logger.error("Unable to unregister " + MBEAN_AUTO_INCR + " mBean", e);
		}

	}

	@Override
    public Properties getProperties() {
		return props;
	}

	@Override
    public String getName() {
		return name;
	}

	@Override
    public String getHostName() {
		return hostName;
	}

    @Override
    public Project getProject() {
		return project;
	}
	
	@Override
    public void setProject(Project p) {
		project = p;
	}

    public DBNative getDBNative() {
        return dbNative;
    }

	@Override
    public CharSetNative getCharSetNative() {
		return charSetNative;
	}

	@Override
    public String getDefaultProjectName() {
		return props.getProperty("defaultproject", Project.DEFAULT);
	}

	@Override
    public List<Pair<String,String>> getStatusVariables(CatalogDAO c) throws PEException {
		List<Pair<String,String>> out = new ArrayList<Pair<String,String>>();
		
		for(VariableInfo<StatusVariableHandler>  vi : statusVariables.getInfoValues()) {
			out.add(new Pair<String,String>(vi.getName(), vi.getHandler().getValue(c, vi.getName())));
		}
		return out;
	}

	@Override
    public String getStatusVariable(CatalogDAO c, String variableName) throws PEException {
		return statusVariables.getVariableInfo(variableName).getHandler().getValue(c, variableName);
	}
	
	@Override
    public String getGlobalVariable(CatalogDAO c, String variableName) throws PEException {
		return globalConfig.getVariableInfo(variableName).getHandler().getValue(c, variableName);
	}

	@Override
    public GlobalVariableHandler getGlobalVariableHandler(String variableName) throws PEException {
		return globalConfig.getVariableInfo(variableName).getHandler();
	}
	
	@Override
    public void setGlobalVariable(CatalogDAO c, String variableName, String value) throws PEException {
		GlobalVariableHandler handler =  globalConfig.getVariableInfo(variableName).getHandler();
		handler.setValue(c, variableName, value);
		
		synchronized(sessionConfigTemplate) {
			sessionConfigDefaults = null;
		}

        Singletons.require(GroupTopicPublisher.class).publish(new OnGlobalConfigChangeMessage(variableName, value));
	}
	
	@Override
    public Map<String,String> getGlobalVariables(CatalogDAO c) throws PEException {
		HashMap<String, String> out = new HashMap<String,String>();
		
		for(VariableInfo<GlobalVariableHandler>  vi : globalConfig.getInfoValues()) {
			out.put(vi.getName(), vi.getHandler().getValue(c, vi.getName()));
		}
		return out;
	}	
	
	@Override
    public String getServerVersion() {
		// TODO turns out the Drupal installer needs this to be a MySQL version
		return PEConstants.DVE_SERVER_VERSION;
	}

	@Override
    public String getServerVersionComment() {
		if(versionComment == null) {
			versionComment = PEConstants.DVE_SERVER_VERSION_COMMENT + ", " + 
					PELogUtils.getBuildVersionString(false) + " (" + getHostName() + ")" ;
		}
		return versionComment;
	}

	@Override
    public String getDveVersion(SSConnection ssCon) throws PEException {
		if (dveVersion == null) {
			dveVersion = getGlobalVariable(ssCon.getCatalogDAO(), "version"); 
		}
		return dveVersion;
	}
	
	@Override
    public String getDveServerVersion() {
		return getServerVersion();
	}

	@Override
    public String getDveServerVersionComment() {
		return getServerVersionComment();
	}

    @Override
    public int getPortalPort(final Properties props) {
        return Integer.parseInt(props.getProperty(
                PEConstants.MYSQL_PORTAL_PORT_PROPERTY,
                PEConstants.MYSQL_PORTAL_DEFAULT_PORT));
    }

	private VariableConfig<ScopedVariableHandler> getScopedConfig(String providerName) throws PEException {
		if (scopedVariables.containsKey(providerName))
			return scopedVariables.get(providerName);

		throw new PEException("No variable configuration defined for provider " + providerName);
	}
	
	@Override
    public String getScopedVariable(String scopeName, String variableName) throws PEException {
		return getScopedConfig(scopeName).getVariableInfo(variableName).getHandler().getValue(scopeName, variableName);
	}
	
	@Override
    public Collection<String> getScopedVariableScopeNames() {
		return scopedVariables.keySet();
	}
	
	@Override
    public Map<String, String> getScopedVariables(String scopeName) throws PEException {
		HashMap<String, String> out = new HashMap<String,String>();

		for(VariableInfo<ScopedVariableHandler> vi : getScopedConfig(scopeName).getInfoValues()) {
			out.put(vi.getName(), vi.getHandler().getValue(scopeName,vi.getName()));
		}
		return out;
	}
	
	@Override
    public void setScopedVariable(String scopeName, String variableName, String value) throws PEException {
		getScopedConfig(scopeName).getVariableInfo(variableName).getHandler().setValue(scopeName, variableName, value);
	}
	
	@Override
    public void addScopedConfig(String scopeName, VariableConfig<ScopedVariableHandler> variableConfig) {
		scopedVariables.put(scopeName, variableConfig);
	}

	@Override
    public VariableConfig<SessionVariableHandler> getSessionConfigTemplate() {
		return sessionConfigTemplate;
	}
	
	@Override
    public VariableValueStore getSessionConfigDefaults(SSConnection ssCon) throws PEException {
		synchronized(sessionConfigTemplate) {
			if(sessionConfigDefaults == null) {
				sessionConfigDefaults = new VariableValueStore("DefaultSession");

				for (Entry<String, VariableInfo<SessionVariableHandler>> vEntry : sessionConfigTemplate.getInfoEntrySet()) {
					try {
						sessionConfigDefaults.initialiseVariableWithKey(vEntry.getKey(), vEntry.getValue().getHandler().getDefaultValue(ssCon));
					} catch (Throwable e) {
						throw new PEException("Looking up default value for variable " + vEntry.getValue().getName() + " by handler " + 
								vEntry.getValue().getHandler().getClass().getSimpleName(), e);
					}
				}
			}
		}
		return sessionConfigDefaults;
	}

	@Override
    public InformationSchemas getInformationSchema() {
		return infoSchema;
	}

	@Override
    public long getDBConnectionTimeout() {
		if (dbConnectionTimeout  < 0) {
			dbConnectionTimeout = Integer.parseInt(props.getProperty(TIMEOUT_KEY, TIMEOUT_DEFAULT));
			if (dbConnectionTimeout < 1000)
				dbConnectionTimeout *= 1000;
		}
		return dbConnectionTimeout;
	}

	@Override
    public TimeBasedGenerator getUuidGenerator() {
		if (uuidGenerator == null) {
			EthernetAddress nic = EthernetAddress.fromInterface();
			if (nic == null) {
				// construct address with a random number
				nic = EthernetAddress.constructMulticastAddress();
			}
			uuidGenerator = Generators.timeBasedGenerator(nic);
		}
		return uuidGenerator;
	}

    @Override
    public long getServerUptime() {
		return ( System.currentTimeMillis() - startTime ) /1000;
	}

	@Override
    public void resetServerUptime() {
		startTime = System.currentTimeMillis();
	}

	@Override
    public ExecutorService getExecutorService() {
		return executorService;
	}
	
	@Override
    public <T> Future<T> submit(Callable<T> task) {
		return executorService.submit(task);
	}

	@Override
    public <T> Future<T> submit(String name, Callable<T> task) {
		return executorService.submit(name, task);
	}
	
	@Override
    public void execute(Runnable task) {
		executorService.execute(task);
	}

	@Override
    public void execute(String name, Runnable task) {
		executorService.execute(name, task);
	}

	
	@Override
    public void onGarbageEvent() {
		// does nothing by default
	}
}
