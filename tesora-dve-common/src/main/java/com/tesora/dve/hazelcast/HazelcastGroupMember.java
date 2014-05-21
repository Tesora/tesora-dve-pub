// OS_STATUS: public
package com.tesora.dve.hazelcast;

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

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.tesora.dve.common.PEConstants;

public abstract class HazelcastGroupMember {

	protected static final String HAZELCAST_INSTANCE_NAME = "tesora_dve";
	protected static final String HAZELCAST_GROUP_NAME = "tesora_dve";
	protected static final String HAZELCAST_GROUP_PASSWORD = "tesora_dve-pass";

	static Logger logger = Logger.getLogger(HazelcastGroupMember.class);

	private String dbURL;
	private String userId;
	private String password;
	protected String catalog;

	public HazelcastGroupMember() {
	}

	protected abstract HazelcastInstance getOurHazelcastInstance();

	public void shutdown() {
		getOurHazelcastInstance().getLifecycleService().shutdown();
	}

	public List<InetSocketAddress> getMembers() {
		List<InetSocketAddress> members = new ArrayList<InetSocketAddress>();
		for(Member m : getOurHazelcastInstance().getCluster().getMembers())
			members.add(m.getInetSocketAddress());
		return members;
	}

	protected ConcurrentMap<InetSocketAddress, InetSocketAddress> getPEServerAddressMap() {
		return getOurHazelcastInstance().getMap("ServerAddressMap");
	}

	public InetSocketAddress getPEServerAddress(InetSocketAddress clusterAddress) {
		return getPEServerAddressMap().get(clusterAddress);
	}

	protected List<String> findAllRegisteredServers(Properties props) throws Exception {
		Connection con = null;
		try {
			con = getDBConnection(props);
			return findAllRegisteredServers(con);
		} finally {
			if (con != null)
				con.close();
		}
	}

	protected List<String> findAllRegisteredServers(Connection con) throws Exception {
		List<String> servers = new ArrayList<String>();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("select ipAddress from " + catalog + ".server");
		while(rs.next())
			servers.add(rs.getString("ipAddress"));
		stmt.close();
		return servers;
	}

	public void removeServerRecord(String ourIPAddress) {
		try {
			Connection con = DriverManager.getConnection(dbURL,userId,password);
			try {
				Statement stmt = con.createStatement();
				stmt.executeUpdate("delete from " + catalog + ".server where ipAddress = '" + ourIPAddress + "'");
				stmt.close();
				logger.info("Unregistered server: " + ourIPAddress);
			} finally {
				con.close();
			}
		} catch (SQLException e) {
			logger.error("Exception encountered removing server registration record", e);
		}
	}

	protected Connection getDBConnection(Properties props) throws ClassNotFoundException, SQLException {
		String driverManagerName = props.getProperty(PEConstants.PROP_FULL_JDBC_DRIVER, PEConstants.MYSQL_DRIVER_CLASS);
		dbURL = props.getProperty(PEConstants.PROP_FULL_JDBC_URL);
		userId = props.getProperty(PEConstants.PROP_FULL_JDBC_USER);
		password = props.getProperty(PEConstants.PROP_FULL_JDBC_PASSWORD);
		catalog = props.getProperty(PEConstants.PROP_DBNAME, PEConstants.CATALOG);

		Class.forName(driverManagerName);
		return DriverManager.getConnection(dbURL, userId, password);
	}

}
