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
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.management.DynamicMBean;
import javax.management.ObjectName;

import com.tesora.dve.charset.*;
import com.tesora.dve.clock.*;
import com.tesora.dve.membership.GroupTopicPublisher;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.server.global.BootstrapHostService;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.global.StatusVariableService;
import com.tesora.dve.sql.ErrorHandlingService;
import com.tesora.dve.sql.schema.ErrorHandlingServiceImpl;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.variables.*;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.InformationSchemaService;
import org.apache.log4j.Logger;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PELogUtils;
import com.tesora.dve.common.catalog.AutoIncrementTrackerDynamicMBean;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.comms.client.messages.ConnectRequest;
import com.tesora.dve.concurrent.NamedTaskExecutorService;
import com.tesora.dve.concurrent.PEThreadPoolExecutor;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupMessageEndpoint;
import com.tesora.dve.sql.infoschema.InformationSchemas;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.upgrade.CatalogVersions;
import com.tesora.dve.variable.status.StatusVariableHandler;
import com.tesora.dve.variable.status.StatusVariableHandlerDynamicMBean;
import com.tesora.dve.variable.status.StatusVariables;

public class Host implements HostService, StatusVariableService, RootProxyService, VariableService {
    protected static Logger logger = Logger.getLogger(Host.class);

	static {
		//this is done statically, to install it if someone just touches the Host class, even indirectly.
		Singletons.replace(BehaviorConfiguration.class,new DefaultBehaviorConfiguration());
		Singletons.replace(ErrorHandlingService.class, new ErrorHandlingServiceImpl() );
	}

	private static final String MBEAN_GLOBAL_CONFIG = "com.tesora.dve:name=ConfigVariables";
	private static final String MBEAN_GLOBAL_STATUS = "com.tesora.dve:name=StatusVariables";
	private static final String MBEAN_AUTO_INCR  =  "com.tesora.dve:name=AutoIncrementTracker";

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
	
	private Properties props;

	private final VariableManager variables;
	
	private VariableHandlerDynamicMBean globalConfigMBean = new VariableHandlerDynamicMBean();

	private Map<String /* providerName */, ScopedVariables> scopedVariables	= new HashMap<String, ScopedVariables>();

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
		Singletons.replace(StatusVariableService.class,singleton);
		Singletons.replace(RootProxyService.class,singleton);
		Singletons.replace(VariableService.class,singleton);

        if (this instanceof BootstrapHost){
            //dirty, but works around issue where initializers were accessing Bootstrap singleton before this constructor exited and bootstrap constructor had time to register sub-interface. -sgossard
            Singletons.replace(BootstrapHostService.class, (BootstrapHost)singleton);
        }

        this.executorService = new PEThreadPoolExecutor("Host");
        
		try {
			this.variables = VariableManager.getManager();
			ServerGlobalVariableStore.INSTANCE.reset();
			hostName = InetAddress.getLocalHost().getHostName();
			
			if (name == null) {
				name = hostName;
			}
			
			CatalogVersions.catalogVersionCheck(props);
			
			DBType dbType = DBType.fromDriverClass(props.getProperty(DBHelper.CONN_DRIVER_CLASS));

			DBNative dbNative = DBNative.DBNativeFactory.newInstance(dbType);
			Singletons.replace(DBNative.class,dbNative);
			Singletons.replace(NativeCharSetCatalog.class,dbNative.getSupportedCharSets());
			Singletons.replace(NativeCollationCatalog.class,dbNative.getSupportedCollations());
			boolean startCatalog = true;
			Singletons.replace(CharSetNative.class, initializeCharsetNative(dbType, startCatalog));//TODO: the only purpose of CharSetNative appears to be holding an alternate NativeCharSetCatalog? -sgossard.

			// we have to set this up early for the variables
			broadcaster = new GroupMessageEndpoint("broadcast");
            Singletons.replace(GroupTopicPublisher.class, broadcaster);
			
			CatalogDAO c = CatalogDAOFactory.newInstance();
			try {
				project = c.findProject(props.getProperty("defaultproject", Project.DEFAULT));
				
				// bootstrap host has already ensured we are part of a cluster if need be, therefore it
				// is safe to initialize variables here.
				variables.initialize(c);
				variables.initializeDynamicMBeanHandlers(globalConfigMBean);

				infoSchema = InformationSchemas.build(dbNative, c, props);
				Singletons.replace(InformationSchemaService.class,infoSchema);

			} finally {
				c.close();
			}
			
			startTime = System.currentTimeMillis();
			
		} catch (Throwable e) {
            throw new RuntimeException("Failed to start DVE server - " + e.getMessage(), e);
		}
	}

	private static CharSetNative initializeCharsetNative(DBType dbType, boolean startCatalog) throws PEException {
		CharSetNative charSetNative = null;

		NativeCharSetCatalog catalog;
		switch (dbType) {
            case MYSQL:
            case MARIADB:
                if (startCatalog) {
                    catalog = new NativeCharSetCatalogImpl();
					if ( !CatalogDAOFactory.isSetup() )
                        throw new PEException("Cannot load character sets from the catalog as the catalog is not setup.");

					CatalogDAO catalog1 = CatalogDAOFactory.newInstance();
					try {
                        DBNative.loadFromCatalog(catalog1, catalog);
                    } finally {
                        catalog1.close();
                    }
				} else {
                    catalog = MysqlNativeCharSetCatalog.DEFAULT_CATALOG;
                    //don't load, it's already populated correctly.
                }
                break;
            default:
                throw new PEException("Attempt to create new character set native instance using invalid database type " + dbType);
        }
		charSetNative = new CharSetNativeImpl(catalog);
		return charSetNative;
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
			variables = VariableManager.getManager();
			ServerGlobalVariableStore.INSTANCE.reset();
			hostName = InetAddress.getLocalHost().getHostName();
			
			if ( startCatalog )
				CatalogDAOFactory.setup(props);

			String driver = DBHelper.loadDriver(props.getProperty(DBHelper.CONN_DRIVER_CLASS));
			
			DBType dbType = DBType.fromDriverClass(driver);
			DBNative dbNative = DBNative.DBNativeFactory.newInstance(dbType);
			Singletons.replace(DBNative.class,dbNative);
			Singletons.replace(NativeCharSetCatalog.class,dbNative.getSupportedCharSets());
			Singletons.replace(NativeCollationCatalog.class,dbNative.getSupportedCollations());
			Singletons.replace(CharSetNative.class, initializeCharsetNative(dbType, startCatalog));//TODO: the only purpose of CharSetNative appears to be holding an alternate NativeCharSetCatalog? -sgossard.
			
			infoSchema = InformationSchemas.build(dbNative,null,props);
			Singletons.replace(InformationSchemaService.class, infoSchema);
			if (startCatalog)
				variables.initialize(null);
		} catch (Exception e) {
            throw new RuntimeException("Failed to start DVE server - " + e.getMessage(), e);
		}
		singleton = this;
        Singletons.replace(HostService.class, singleton);
		Singletons.replace(StatusVariableService.class,singleton);
		Singletons.replace(RootProxyService.class,singleton);
		Singletons.replace(VariableService.class,singleton);
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


    protected Project getProject() {
		return project;
	}

	@Override
    public String getDefaultProjectName() {
		return props.getProperty("defaultproject", Project.DEFAULT);
	}

	@Override
    public List<Pair<String,String>> getStatusVariables() throws PEException {
		List<Pair<String,String>> out = new ArrayList<Pair<String,String>>();

		for(StatusVariableHandler svh : StatusVariables.getStatusVariables()) {
			out.add(new Pair<String,String>(svh.getName(),svh.getValue()));
		}
		
		return out;
	}

	@Override
    public String getStatusVariable(String variableName) throws PEException {
		return StatusVariables.lookup(variableName, true).getValue();
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

    public ScopedVariables getScopedConfig(String providerName) throws PEException {
		if (scopedVariables.containsKey(providerName))
			return scopedVariables.get(providerName);

		throw new PEException("No variable configuration defined for provider " + providerName);    	
    }

	@Override
    public Collection<String> getScopedVariableScopeNames() {
		return scopedVariables.keySet();
	}
	
	@Override
    public Map<String, String> getScopedVariables(String scopeName) throws PEException {
		HashMap<String, String> out = new HashMap<String,String>();

		for(ScopedVariableHandler svh : getScopedConfig(scopeName).getHandlers()) {
			out.put(svh.getName(),svh.getValue());
		}
		return out;
	}
	
	@Override
    public void setScopedVariable(String scopeName, String variableName, String value) throws PEException {
		getScopedConfig(scopeName).getScopedVariableHandler(variableName).setValue(value);
	}
	
	@Override
    public void addScopedConfig(String scopeName, ScopedVariables variableConfig) {
		if (variableConfig == null) return;
		scopedVariables.put(scopeName,variableConfig);
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

	@Override
	public VariableManager getVariableManager() {
		return variables;
	}

	@Override
	public SSConnectionProxy getRootProxy() throws PEException {
		User rootUser = getProject().getRootUser();
		
		SSConnectionProxy ssConProxy = new SSConnectionProxy();
		try {
			ssConProxy.executeRequest(new ConnectRequest(rootUser.getName(), rootUser.getPlaintextPassword()));
		} catch (PEException pe) {
			ssConProxy.close();
			return null;
		}
		return ssConProxy;
	}
}
