// OS_STATUS: public
package com.tesora.dve.groupmanager;

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.lockmanager.LockManager;
import com.tesora.dve.variable.GlobalConfigVariableConstants;

public class GroupManager {
	
	public static final String HIBERNATE_CACHE_REGION_FACTORY_CLASS = "hibernate.cache.region.factory_class";

	public static final String DVE_GROUP_SERVICE = "GroupService";

	public static final String DVE_COORDINATION_SERVICES = "tesora.dve.coordinationServices";

	static Logger logger = Logger.getLogger(GroupManager.class);

	static Map<String, CacheConfigurator.Factory> cacheMap = new HashMap<String, CacheConfigurator.Factory>() {
		private static final long serialVersionUID = 1L;
		{
			put(DefaultCacheConfigurator.TYPE, new DefaultCacheConfigurator.Factory());
			put(EHCacheCacheConfigurator.TYPE1, new EHCacheCacheConfigurator.Factory());
			put(EHCacheCacheConfigurator.TYPE2, new EHCacheCacheConfigurator.Factory());
			put(HazelcastCacheConfigurator.TYPE, new HazelcastCacheConfigurator.Factory());
		}
	};
			
	static Map<String, CoordinationServices.Factory> groupMap = new HashMap<String, CoordinationServices.Factory>() {
		private static final long serialVersionUID = 1L;
		{
			put(LocalhostCoordinationServices.TYPE, new LocalhostCoordinationServices.Factory());
			put(HazelcastCoordinationServices.TYPE, new HazelcastCoordinationServices.Factory());
		}
	};
	
	public static boolean isValidServiceType(String serviceType) {
		return groupMap.containsKey( serviceType.toLowerCase(Locale.ENGLISH) );
	}
	
	static CacheConfigurator cacheConfigurator;
	static CoordinationServices coordinationServices;
	
	public static void initialize(Properties props) {
		String groupServicesType = LocalhostCoordinationServices.TYPE; 

		String driverManagerName = props.getProperty(DBHelper.CONN_DRIVER_CLASS);
		String url = props.getProperty(DBHelper.CONN_URL);
		String userId = props.getProperty(DBHelper.CONN_USER);
		String password = props.getProperty(DBHelper.CONN_PASSWORD);
		String database = props.getProperty(DBHelper.CONN_DBNAME);
		
		try { 
			Class.forName(driverManagerName);
			Connection con = DriverManager.getConnection(url,userId,password);
			try {
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery("select value from " + database + ".config where name = '" + GlobalConfigVariableConstants.GROUP_SERVICE + "'");
				while(rs.next())
					groupServicesType = rs.getString("value");
				stmt.close();
			} finally {
				con.close();
			}
		} catch (Exception e) {
			logger.warn("Unable to determine group services status from catalog at '" + url + "'", e);
		}

		coordinationServices = groupMap.get(groupServicesType.toLowerCase(Locale.ENGLISH)).newInstance();
		coordinationServices.configureProperties(props);
		
		cacheConfigurator = cacheMap.get(props.getProperty(HIBERNATE_CACHE_REGION_FACTORY_CLASS, 
						DefaultCacheConfigurator.TYPE)).newInstance();

		logger.info("GroupManager: CacheConfigurator: " + cacheConfigurator.getClass().getSimpleName());
	}
	
	public static void shutdown() {
		if (cacheConfigurator != null) {
			cacheConfigurator.shutdown();
			cacheConfigurator = null;
		}
		if (coordinationServices != null) {
			coordinationServices.shutdown();
			coordinationServices = null;
            Singletons.unregister(LockManager.class);
		}
	}

	public static CacheConfigurator getCacheConfigurator() {
		return cacheConfigurator;
	}

	public static CoordinationServices getCoordinationServices() {
		return coordinationServices;
	}

}
