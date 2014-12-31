package com.tesora.dve.externalservice;

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

import java.sql.ResultSet;
import java.sql.SQLException;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.dbc.ServerDBConnection;
import com.tesora.dve.dbc.ServerDBConnectionParameters;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.template.TemplateBuilder;

public class ExternalServiceContextImpl implements ExternalServiceContext {
	Logger logger = Logger.getLogger(ExternalServiceContextImpl.class);
	
	// TODO should be configurable? It seems that if we set it short enough to be below
	//      any DB idle timeout, we should be ok
	private static final long IDLE_CONNECT_TIMEOUT = 300;
	
	String externalServiceName;
	private ServerDBConnection svrDBconn = null;
	private ExternalService externalService = null;
	private long connectIdleTimerStart = 0;
	private ServerDBConnectionParameters svrDBParams = null;

	public ExternalServiceContextImpl(String externalServiceName) throws PEException {
		this.externalServiceName = externalServiceName;

		// If this External Service needs a DataStore, check if the Database
		// needs to be created on the System SG
		if (getExternalService().usesDataStore()) {
			CatalogDAO c = null;
			externalService = null;
			try {
				c = CatalogDAOFactory.newInstance();
				DBNative dbNat = Singletons.require(DBNative.class);
				if (c.findDatabase(getExternalService().getDataStoreName(), false) == null) {
					// see if we already have an allRandom template, and if so, don't bother creating it
					if (!hasAllRandom(getServerDBConnection()))
						getServerDBConnection().execute(new TemplateBuilder("allRandom").withTable(".*", "Random").toCreateStatement());
					getServerDBConnection().execute(
							String.format("create database %s default persistent group %s using template allRandom default character set = %s default collate = %s",
									getExternalService().getDataStoreName(),
									PEConstants.SYSTEM_GROUP_NAME,
									dbNat.getDefaultServerCharacterSet(),
									dbNat.getDefaultServerCollation()));
				}

			} catch (Throwable e) {
				throw new PEException("Unable to create service datastore for "
						+ externalServiceName, e);
			} finally {
				if (c != null)
					c.close();
			}
		}
	}

	private static boolean hasAllRandom(ServerDBConnection conn) {
		ResultSet rs = null;
		boolean any = false;
		try {
			rs = conn.executeQuery("select name from information_schema.templates where name = 'allRandom'");
			while(rs.next()) 
				any = true;
		} catch (SQLException sqle) {
			return false;
		} finally {
			if (rs != null) try {
				rs.close();
			} catch (SQLException sqle) {
				// really?
			}
		}
		return any;
	}
	
	@Override
	public void setServiceAutoStart(final boolean autoStart) throws PEException {
		CatalogDAO c = null;
		externalService = null;
		try {
			c = CatalogDAOFactory.newInstance();
			final CatalogDAO cdf = c;
			c.new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					ExternalService es = cdf.findExternalService(externalServiceName);
					es.setAutoStart(autoStart);
					return es;
				}
			}.execute();
		} catch (Throwable e) {
			throw new PEException("Unable to update external service " + externalServiceName, e);
		} finally {
			if ( c != null )
				c.close();
		}
	}

	@Override
	public boolean getServiceAutoStart() throws PEException {
		return getExternalService().isAutoStart();
	}

	@Override
	public void setServiceConfig(final ExternalServiceConfig esConfig) throws PEException {
		final String config = esConfig.marshall();
		externalService = null;
		CatalogDAO c = null;
		try {
			c = CatalogDAOFactory.newInstance();
			final CatalogDAO cdf = c;
			c.new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					ExternalService es = cdf.findExternalService(externalServiceName);
					es.setConfig(config);
					return es;
				}
			}.execute();
		} catch (Throwable e) {
			throw new PEException("Unable to update external service " + externalServiceName, e);
		} finally {
			if ( c != null )
				c.close();
		}
	}

	@Override
	public void getServiceConfig(ExternalServiceConfig esConfig) throws PEException {
		esConfig.unmarshall(getExternalService().getConfig());
	}

	@Override
	public String getServiceName() throws PEException {
		return getExternalService().getName();
	}

	@Override
	public String getPlugin() throws PEException {
		return getExternalService().getPlugin();
	}
	
	boolean isServerDBConnectionTimedOut() {
		return ( ( System.currentTimeMillis() - connectIdleTimerStart ) / 1000 ) <= IDLE_CONNECT_TIMEOUT;
	}
	
	public ServerDBConnection getServerDBConnection() throws SQLException, PEException {
		if ((svrDBconn == null) || (!isServerDBConnectionTimedOut() && !svrDBconn.isInTransaction())) {
			closeServerDBConnection();
			User connectUser = getConnectUser();
			if ( logger.isDebugEnabled() )
				logger.debug("Creating ServerDBConnection for " + connectUser.getName());
			if (svrDBParams != null)
				svrDBParams.setCacheName(externalServiceName);
			svrDBconn = new ESServerDBConnection(connectUser.getName(), connectUser.getPlaintextPassword(), svrDBParams);
		}
		connectIdleTimerStart = System.currentTimeMillis();
		return svrDBconn;
	}
	
	@Override 
	public void closeServerDBConnection() throws PEException {
		if (svrDBconn != null) {
			svrDBconn.closeComms();
			svrDBconn = null;
		}
	}

	@Override
	public ServerDBConnection useServiceDataStore() throws SQLException, PEException {
		if ( !getExternalService().usesDataStore() )
			throw new PEException("Service " + getServiceName() + " isn't configured to use a DataStore");
		
		getServerDBConnection().setCatalog(getExternalService().getDataStoreName());
		return getServerDBConnection();
	}

	private ExternalService getExternalService() throws PEException {
		if (externalService == null) {
			CatalogDAO c = null;
			try {
				c = CatalogDAOFactory.newInstance();
				return c.findExternalService(externalServiceName);
			} finally {
				if (c != null)
					c.close();
			}
		} else
			return externalService;
	}

	private User getConnectUser() throws PEException {
		String userid = getExternalService().getConnectUser();
		CatalogDAO c = null;
		try {
			c = CatalogDAOFactory.newInstance();
			return c.findUser(userid);
		} finally {
			if (c != null)
				c.close();
		}
	}
		
	@Override
	public ServerDBConnectionParameters getSvrDBParams() {
		return svrDBParams;
	}

	@Override
	public void setSvrDBParams(ServerDBConnectionParameters svrDBParams) {
		this.svrDBParams = svrDBParams;
	}

	@Override
	public void close() throws PEException {
		closeServerDBConnection();
	}
	
	public class ESServerDBConnection extends ServerDBConnection {

		public ESServerDBConnection(String connectUser, String connectPwd)
				throws PEException {
			super(connectUser, connectPwd);
			getSSConn().getReplicationOptions().setConnectionFromReplicationSlave(true);
		}

		public ESServerDBConnection(String connectUser, String connectPwd, ServerDBConnectionParameters svrDBParams) throws PEException {
			super(connectUser, connectPwd, svrDBParams);
			getSSConn().getReplicationOptions().setConnectionFromReplicationSlave(true);
		}
		
	}
}
