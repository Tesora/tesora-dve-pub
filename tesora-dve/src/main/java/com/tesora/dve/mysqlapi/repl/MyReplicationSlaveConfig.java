// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceConfig;

public class MyReplicationSlaveConfig implements ExternalServiceConfig {
	Logger logger = Logger.getLogger(MyReplicationSlaveConfig.class);

	private static String CONFIG_DELIMETER=",";
	private static String REPL_IGNORE_TABLE_KEY = "replicate-ignore-table";
	private static String REPL_INCLUDE_DB_KEY = "replicate-include-db";
	private static String REPL_SLAVE_SKIP_ERRORS = "slave-skip-errors";
	public static String REPL_SLAVE_MASTER_RETRY_CONNECT = "master-retry-connect";
	public static String REPL_SLAVE_MASTER_RETRY_COUNT = "master-retry-count";
	public static String REPL_SLAVE_SLAVE_NET_TIMEOUT = "slave-net-timeout";
	private final static long DEFAULT_MASTER_CONNECT_RETRY_INTERVAL = 60;
	private final static long DEFAULT_MAX_MASTER_RETRY_ATTEMPTS = 86400;
	private final static long DEFAULT_SLAVE_RETRY_TIME_LIMIT = 3600;
	
	private PropertiesConfiguration configProps;
	
	private String masterHost;
	private int masterPort;
	private int slaveServerID;
	private String masterUserid;
	private String masterPwd;
	private String initialBinlogName;
	private long initialBinlogPosition;
	private Set<String> includeDBList=null;
	private String configFile=null;
	private Set<String> ignoreTableList=null;
	private MyReplErrorAdjudicator adjudicator = null;
	private long masterConnectRetry=DEFAULT_MASTER_CONNECT_RETRY_INTERVAL; // in seconds
	private long masterRetryCount=DEFAULT_MAX_MASTER_RETRY_ATTEMPTS;
	private long slaveNetTimeout=DEFAULT_SLAVE_RETRY_TIME_LIMIT; // in seconds
	
	public MyReplicationSlaveConfig() {};
	
	public MyReplicationSlaveConfig(String masterHost, int masterPort, int slaveServerID, String masterUserid,
		String masterPwd, String initialBinlogName, long initialBinlogPosition, String configFile) throws PEException {
		this.masterHost = masterHost;
		this.masterPort = masterPort;
		this.slaveServerID = slaveServerID;
		this.masterUserid = masterUserid;
		this.masterPwd = masterPwd;
		this.initialBinlogName = initialBinlogName;
		this.initialBinlogPosition = initialBinlogPosition;
		this.configFile = configFile;
		loadConfigFile();
	}

	public String getMasterHost() {
		return masterHost;
	}

	public void setMasterHost(String masterHost) {
		this.masterHost = masterHost;
	}
	
	public int getMasterPort() {
		return masterPort;
	}
	
	public void setMasterPort(int masterPort) {
		this.masterPort = masterPort;
	}
	
	public String getInitialBinlogName() {
		return initialBinlogName;
	}
	
	public void setInitialBinlogName(String initialBinlogName) {
		this.initialBinlogName = initialBinlogName;
	}
	
	public long getInitialBinlogPosition() {
		return initialBinlogPosition;
	}
	
	public void setInitialBinlogPosition(long initialBinlogPosition) {
		this.initialBinlogPosition = initialBinlogPosition;
	}
	
	public int getSlaveServerID() {
		return slaveServerID;
	}
	
	public void setSlaveServerID(int slaveServerID) {
		this.slaveServerID = slaveServerID;
	}

	public String getMasterUserid() {
		return masterUserid;
	}

	public void setMasterUserid(String masterUserid) {
		this.masterUserid = masterUserid;
	}

	public String getMasterPwd() {
		return masterPwd;
	}

	public void setMasterPwd(String masterPwd) {
		this.masterPwd = masterPwd;
	}

	public MyBinLogPosition getInitialMyBinLogPosition() {
		return new MyBinLogPosition(masterHost, initialBinlogName, initialBinlogPosition);
	}

	public Set<String> getIgnoreTableList() {
		if ( configFile == null ) 
			return Collections.emptySet();
	
		if ( ignoreTableList != null )
			return ignoreTableList;
		
		ignoreTableList = new HashSet<String>(Arrays.asList(configProps.getStringArray(REPL_IGNORE_TABLE_KEY)));
			
		return ignoreTableList;
	}
	
	public Set<String> getIncludeDBList() {
		if ( configFile == null ) 
			return Collections.emptySet();
	
		if ( includeDBList != null)
			return includeDBList;
		
		includeDBList = new HashSet<String>(Arrays.asList(configProps.getStringArray(REPL_INCLUDE_DB_KEY)));
			
		return includeDBList;
	}
	
	public MyReplErrorAdjudicator getReplErrorAdjudicator() {
		if (adjudicator == null) {
			adjudicator = new MyReplErrorAdjudicator();
		}
		return adjudicator;
	}

	public long getMasterConnectRetry() {
		return masterConnectRetry;
	}

	public long getMasterRetryCount() {
		return masterRetryCount;
	}
	
	public long getSlaveNetTimeout() {
		return slaveNetTimeout;
	}

	private void loadConfigFile() throws PEException {
		if ( configFile != null && configProps == null ) {
			configProps = PEFileUtils.loadPropertiesConfigFromClasspath(getClass(), configFile);
			
			if (adjudicator == null) {
				adjudicator = new MyReplErrorAdjudicator(new LinkedHashSet<String>(Arrays.asList(configProps.getStringArray(REPL_SLAVE_SKIP_ERRORS))));
			}

			masterConnectRetry = configProps.getLong(REPL_SLAVE_MASTER_RETRY_CONNECT, DEFAULT_MASTER_CONNECT_RETRY_INTERVAL);
			masterRetryCount = configProps.getLong(REPL_SLAVE_MASTER_RETRY_COUNT, DEFAULT_MAX_MASTER_RETRY_ATTEMPTS);
			slaveNetTimeout = configProps.getLong(REPL_SLAVE_SLAVE_NET_TIMEOUT, DEFAULT_SLAVE_RETRY_TIME_LIMIT);
		}
	}
	
	@Override
	public void unmarshall(String config) throws PEException {
		StringTokenizer st = new StringTokenizer(config, CONFIG_DELIMETER);

		String currentToken=null, currentValue=null;
		try {
			currentToken = "Host";
			masterHost = st.nextToken().trim();

			currentToken = "Port";
			currentValue = st.nextToken().trim();
			masterPort = Integer.valueOf(currentValue);
			
			currentToken = "Userid";
			masterUserid = st.nextToken().trim();
			currentToken = "Password";
			masterPwd = st.nextToken().trim();
			
			currentToken = "Slave ID";
			currentValue = st.nextToken().trim();
			slaveServerID = Integer.valueOf(currentValue);

			currentToken = "Initial Binlog Filename";
			initialBinlogName = st.nextToken().trim();
			
			currentToken = "Initial Binlog Position";
			currentValue = st.nextToken().trim();
			initialBinlogPosition = Integer.valueOf(currentValue);
			
		} catch (NoSuchElementException nse) {
			throw new PEException(
					"Configuration string for MyReplicationSlaveService doesn't contain enough values",nse);
		} catch (NumberFormatException nfe) {
			throw new PEException("Configuration value for " + currentToken
					+ " must be numeric - received '" + currentValue + "'", nfe);
		}

		// if the next token exists it is the configFile
		if ( st.hasMoreTokens() ) {
			configFile = st.nextToken().trim();
			loadConfigFile();
		}
			
	}

	@Override
	public String marshall() {
		StringBuilder configOut = new StringBuilder().append(masterHost).append(CONFIG_DELIMETER)
				.append(masterPort).append(CONFIG_DELIMETER)
				.append(masterUserid).append(CONFIG_DELIMETER)
				.append(masterPwd).append(CONFIG_DELIMETER)
				.append(slaveServerID).append(CONFIG_DELIMETER)
				.append(initialBinlogName).append(CONFIG_DELIMETER)
				.append(initialBinlogPosition);
				
		if ( configFile != null )
			configOut.append(CONFIG_DELIMETER).append(configFile);
		
		return configOut.toString();
	}
}
