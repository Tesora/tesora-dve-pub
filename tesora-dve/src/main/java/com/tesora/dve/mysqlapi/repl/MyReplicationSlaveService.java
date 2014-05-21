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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.MyLoadDataInfileContext;
import com.tesora.dve.dbc.ServerDBConnection;
import com.tesora.dve.dbc.ServerDBConnectionParameters;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceContext;
import com.tesora.dve.externalservice.ExternalServiceContextImpl;
import com.tesora.dve.externalservice.ExternalServicePlugin;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.groupmanager.GroupMembershipListener.MembershipEventType;
import com.tesora.dve.mysqlapi.MyClientConnectionContext;

public class MyReplicationSlaveService implements ExternalServicePlugin {
	Logger logger = Logger.getLogger(MyReplicationSlaveService.class);

	ExternalServiceContextImpl esContext;
	MyReplSlaveClientConnection myClient;
	String name;
	MyReplicationSlaveConfig myConfig = new MyReplicationSlaveConfig();
	MyReplSessionVariableCache sessionVariableCache = new MyReplSessionVariableCache();
	MyInfileHandler infileHandler = new MyInfileHandler();
	MyBinLogDAO binLogDAO = new MyBinLogDAO();
	MyReplSlaveState replSlaveState;
	MyBinLogPosition currentBLPos = null;
	Thread reconnectThread = null;  //hmm does it need synchronization...
	MyLoadDataInfileContext replSlaveLoadDataConnectionContext = new MyLoadDataInfileContext(); 
			
	boolean isStarted = false;
	boolean stopCalled = false;

	long lastEventTimestamp = 0;
	
	@Override
	public void initialize(ExternalServiceContext ctxt) throws PEException {
		stopCalled = false;
		this.esContext = (ExternalServiceContextImpl)ctxt;

		// get the current configuration loaded into myConfig
		ctxt.getServiceConfig(myConfig);
		
		// get the tables to ignore list from the Config
		ServerDBConnectionParameters svrDBParams = new ServerDBConnectionParameters();
		svrDBParams.setReplSlaveIgnoreTblList(myConfig.getIgnoreTableList());
		
		if ( !svrDBParams.getReplSlaveIgnoreTblList().isEmpty() ) 
			ctxt.setSvrDBParams(svrDBParams);

		// validate that our datastore is configured
		try {
			validateDataStore();
		} catch (SQLException e) {
			throw new PEException("Error occurred validating service DataStore for "
					+ ctxt.getServiceName(),e);
		}

		if (logger.isDebugEnabled())
			logger.debug("External Service " + getName() + " is initialized");
	}

	@Override
	public void start() throws PEException {
		start(true);
	}

	public void start(boolean withCleanup) throws PEException {
		stopCalled = false;
		
		if (myClient != null && myClient.isConnected()) {
			throw new PEException(getName() + " service is already started.");
		}

		try {
			// this is used to facilitate a unit test - test will instantiate myClient
			if (myClient == null)
				instantiateMyClient();

			// this is to facilitate a unit test - test will pass in a dummy MyReplSlaveState
			if (replSlaveState == null)
				replSlaveState = new MyReplSlaveState(this, myClient);

			replSlaveState.handShake();

		} catch (Exception e) {
			if (withCleanup) {
				// clean up
				stop();
			}
			throw new PEException("Failed to start '" + getName() + "' service.", e);
		}

		if (logger.isDebugEnabled())
			logger.debug("External Service " + getName() + " has been started");

		isStarted = true;
	}

	@Override
	public void stop() {
		stopCalled = true;
		if ( logger.isDebugEnabled() )
			logger.debug("Stop called for External Service " + getSlaveInfo());

		if (reconnectThread != null) {
			if ( logger.isDebugEnabled() ) {
				logger.debug("Replication slave reconnection is about to be terminated by STOP service command.");
			}
			reconnectThread.interrupt();
			reconnectThread = null;
		}
		if (myClient != null) {
			myClient.stop();
			myClient = null;
			replSlaveState = null;
		}
		try {
			esContext.closeServerDBConnection();
		} catch (PEException e) {
			// TODO - for now eat this...
			logger.error("Error during stop",e);
		}

		isStarted = false;
	}

	@Override
	public String status() throws PEException {
		StringBuilder status = new StringBuilder("Status for: " + getName());

		if ((myClient != null) && myClient.isConnected())
			status.append(" - STARTED");
		else
			status.append(" - STOPPED");
		
		status.append(" - Current binlog position is: ");
		try {
			MyBinLogPosition blp = getBinLogPosition();
			status.append(blp.toString());
		} catch (SQLException e) {
			// if we can't find the binlog_status we are going to report it as unknown
			status.append("<UNKNOWN>");
		}
		
		status.append(" - Timestamp on last event was ");
		if ( lastEventTimestamp > 0 ) {
			status.append((System.currentTimeMillis()/1000) - lastEventTimestamp)
				.append(" seconds ago");
		} else {
			status.append("<UNKNOWN>");
		}
		
		return status.toString();
	}

	@Override
	public String getName() throws PEException {
		if (name == null)
			name = esContext.getServiceName();
		return name;
	}

	@Override
	public String getPlugin() throws PEException {
		return esContext.getPlugin();
	}
	
	@Override
	public void close() {
		stop();
		if (esContext != null) {
			try {
				esContext.close();
			} catch (PEException e) {
				// do nothing
			}
		}
	}

	void instantiateMyClient() {
		myClient = new MyReplSlaveClientConnection(
				new MyReplSlaveConnectionContext(getMasterHost(), getMasterPort()), 
				this);
	}

	protected void validateDataStore() throws SQLException, PEException {
		boolean valid = false;
		ServerDBConnection conn = esContext.useServiceDataStore();

		try {
			valid = binLogDAO.doesBinLogStatusTableExist(conn);
	
			if (!valid) {
				if (logger.isDebugEnabled())
					logger.debug("External Service " + getName()
							+ " couldn't locate DataStore tables...creating...");
				binLogDAO.createBinLogStatusTable(conn);
				// populate initial binlog position
				updateBinLogPosition(getInitialMyBinLogPosition());
			}
		} finally {
			esContext.closeServerDBConnection();
		}
	}

	@Override
	public void reload() throws PEException {
		try {
			// reload from the context first to update any entities such as the config
			initialize(this.esContext);
			
			MyBinLogPosition blp = new MyBinLogPosition(getMasterHost(),
					getInitialBinlogName(), getInitialBinlogPosition());
			currentBLPos = blp;
			updateBinLogPosition(blp);
		} catch (Exception e) {
			throw new PEException("Error reloading service." ,e);
		}
	}

	@Override
	public void restart() throws PEException {
		stop();

		// initiate reconnection on new thread to allow existing i/o thread to resume channel closed processing
		reconnectThread = new Thread(new Runnable() {

			@Override
			public void run() {
				FastDateFormat formatter = FastDateFormat.getDateTimeInstance(FastDateFormat.MEDIUM, FastDateFormat.MEDIUM);
				
				long retries = 0;
				long start = System.currentTimeMillis();
				long lastAttempt = start;

				logger.info("Replication slave lost connection on " + formatter.format(start) + ".  Attempting to reconnect with the following parameters: " +
						MyReplicationSlaveConfig.REPL_SLAVE_MASTER_RETRY_CONNECT + "=" + myConfig.getMasterConnectRetry() + " seconds" +
						", " + MyReplicationSlaveConfig.REPL_SLAVE_SLAVE_NET_TIMEOUT + "=" + myConfig.getSlaveNetTimeout() + " seconds" +
						", " + MyReplicationSlaveConfig.REPL_SLAVE_MASTER_RETRY_COUNT + "=" + myConfig.getMasterRetryCount());

				while (myClient == null || !myClient.isConnected()) {
					if (Thread.interrupted()) {
						logger.info("Replication slave reconnection was terminated by STOP service command.");
						reconnectThread = null;
						break;
					}
 
					if (((retries > 0) && (retries >= myConfig.getMasterRetryCount()))
							|| ((lastAttempt - start) / 1000) > myConfig.getSlaveNetTimeout()) {
						logger.warn("Replication slave was unable to reconnect and will stop replication.  Total attempts=" + retries + 
								", Started=" + formatter.format(start) +
								", Ended=" + formatter.format(lastAttempt));
						reconnectThread = null;
						return;
					}

					try {
						Thread.sleep(myConfig.getMasterConnectRetry() * 1000);
					} catch (Exception e) {
						logger.info("Replication slave reconnection was terminated by STOP service command.");
						reconnectThread = null;
						return;
					}

					if (Thread.interrupted()) {
						reconnectThread = null;
						logger.info("Replication slave reconnection was terminated by STOP service command.");
						break;
					}
 
					retries++;
					lastAttempt = System.currentTimeMillis();
					try {
						if (logger.isDebugEnabled()) {
							logger.debug("Replication slave reconnect attempt #" + retries + " at "
									+ formatter.format(lastAttempt));
						}
						start(false);
					} catch (Exception e) {
						// do nothing
						if (logger.isDebugEnabled()) {
							logger.debug("Replication slave reconnect attempt #" + retries + " failed.", e);
						}
					}
				}

				if (myClient.isConnected()) {
					// successfully reconnected
					logger.info("Replication slave successfully reconnected on attempt " + retries + 
							" on " + formatter.format(lastAttempt));
				}
			}
			
		}, "ReplicationReconnect");
		reconnectThread.start();
	}
	
	@Override
	public boolean isStarted() {
		return isStarted;
	}

	public ServerDBConnection getServerDBConnection() throws SQLException, PEException {
		ServerDBConnection conn = null;
		if (esContext != null) {
			conn = esContext.getServerDBConnection();
		}
		return conn;
	}

	public ServerDBConnection useServiceDataStore() throws SQLException, PEException {
		ServerDBConnection conn = null;
		if (esContext != null) {
			conn = esContext.useServiceDataStore();
		}
		return conn;
	}

	public boolean includeDatabase(String dbName) {
		return (( getIncludeDBList().isEmpty() ) || ( getIncludeDBList().contains(dbName) ));
	}

	public boolean stopCalled() {
		return stopCalled;
	}
	
	// *** Configuration helper methods ***
	
	public String getMasterHost() {
		return myConfig.getMasterHost();
	}

	public int getMasterPort() {
		return myConfig.getMasterPort();
	}
	
	// convenience method to return <masterHost>:<masterPort>
	public String getMasterLocator() {
		return String.format("%s:%d", getMasterHost(), getMasterPort());
	}

	public String getMasterPwd() {
		return myConfig.getMasterPwd();
	}

	public String getMasterUserid() {
		return myConfig.getMasterUserid();
	}

	public int getSlaveServerID() {
		return myConfig.getSlaveServerID();
	}

	// convenience method to return <name>:<slaveId>
	public String getSlaveInfo()  {
		String rv = null;
		
		try {
			rv = String.format("%s:%d", getName(), getMasterPort());
		} catch (PEException e) {
			// catch this exception and just log an error and return <UNKNOWN>
			logger.error("Exception occurred retrieving plugin name for " + this.toString(), e);
			rv = String.format("%s:%d", "<UNKNOWN>", getMasterPort());
		}
		
		return rv;
	}

	public String getInitialBinlogName() {
		return myConfig.getInitialBinlogName();
	}

	public long getInitialBinlogPosition() {
		return myConfig.getInitialBinlogPosition();
	}

	public MyBinLogPosition getInitialMyBinLogPosition() {
		return myConfig.getInitialMyBinLogPosition();
	}
	
	public Set<String> getIncludeDBList() {
		return myConfig.getIncludeDBList();
	}
	
	public boolean validateErrorAndStop(int masterSqlErrorCode) {
		return myConfig.getReplErrorAdjudicator().validateErrorAndStop(masterSqlErrorCode);
	}

	public boolean validateErrorAndStop(int masterSqlErrorCode, Exception slaveException) {
		return myConfig.getReplErrorAdjudicator().validateErrorAndStop(masterSqlErrorCode, slaveException);
	}
	
	public void setLastEventTimestamp(long timestamp) {
		this.lastEventTimestamp = timestamp;
	}
	
	// *** Datastore helper methods ***
	
	public MyBinLogPosition getBinLogPosition() throws SQLException, PEException {
		if (currentBLPos == null) {
			currentBLPos = binLogDAO.getBinLogPosition(useServiceDataStore(), getMasterHost());
		}

		if ( logger.isDebugEnabled() ) 
			logger.debug("Returning Binlog position " + currentBLPos.toString());
		
		return currentBLPos;

	}

	public void updateBinLogPosition(MyBinLogPosition myBLPos) throws SQLException, PEException {
		binLogDAO.updateBinLogPosition(useServiceDataStore(), myBLPos);
		currentBLPos = myBLPos;
	}

	public MyReplSessionVariableCache getSessionVariableCache() {
		return sessionVariableCache;
	}

	public MyInfileHandler getInfileHandler() {
		return infileHandler;
	}

	public MyLoadDataInfileContext getReplSlaveLoadDataConnectionContext() {
		return replSlaveLoadDataConnectionContext;
	}

	public MyClientConnectionContext getClientConnectionContext() {
		return myClient.getContext();
	}
	
	@Override
	public boolean denyServiceStart(ExternalServiceContext ctxt) throws PEException {
		String serviceAddress = stripPort(GroupManager.getCoordinationServices().getExternalServiceRegisteredAddress(ctxt.getServiceName()));
		InetSocketAddress isa = GroupManager.getCoordinationServices().getMemberAddress();
		InetAddress ia = isa.getAddress();
		String ourAddress = null;
		if (ia == null) {
			ourAddress = stripPort(isa.getHostName());
		} else {
			ourAddress = stripPort(ia.getHostAddress());
		}
		if (!StringUtils.equals(serviceAddress, ourAddress)) {
			// someone else has started replication slave so don't allow start
			if (logger.isDebugEnabled()) {
				logger.debug("Service '" + ctxt.getServiceName() + "' is already registered at '" + serviceAddress + "'");
			}
			return true;
		}

		return false;
	}
	
	private String stripPort(String address) {
		if (address == null)
			return null;
		return StringUtils.substringBeforeLast(address, ":");
	}
	
	@Override
	public void handleGroupMembershipEvent(MembershipEventType eventType,
			InetSocketAddress inetSocketAddress) {
		if (eventType == MembershipEventType.MEMBER_REMOVED) {
			String memberAddress = inetSocketAddress.getHostName();
			try {
				System.out.println("Member removed.  Updating service: " + getName());
				
				String externalServiceAddress = GroupManager.getCoordinationServices().getExternalServiceRegisteredAddress(getName()); 
				
				System.out.println("Member removed.  External service started on (" + externalServiceAddress + ") and machine leaving is (" + memberAddress + ")");
				// for now keep the service registered 

//		if (StringUtils.equals(memberAddress, externalServiceAddress)) {
//			System.out.println("Member removed.  Site starting the service has stopped communicating.");
//
//			// force deregistration of existing service
//			GroupManager.getCoordinationServices().deregisterExternalService(getName());
//
//			if (GroupManager.getCoordinationServices().isInQuorum()) {
//				System.out.println("Member removed.  In quorum starting external services.");
//				try {
//					ExternalServiceFactory.register(getName(), getPlugin());
//				} catch (Exception e) {
//			l		ogger.warn("Failed to register external service '" + getName() + "'", e);
//				}
//			}
//		}
			} catch (Exception e) {
				try {
					logger.warn("Failed to register external service '" + getName() + "'", e);
				} catch (PEException e1) {
				}
			}
		}
	}
}
