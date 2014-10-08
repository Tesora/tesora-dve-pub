package com.tesora.dve.common.catalog;

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
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.groupmanager.GroupMembershipListener;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.Host;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.singleton.Singletons;

@Entity
@Table(name = "user_temp_table")
public class TemporaryTable implements CatalogEntity {

	static Logger logger = Logger.getLogger(TemporaryTable.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name="id")
	int id;

	// the connection visible name
	@Column(name="name", nullable=false)
	String name;

	@Column(name="table_engine",nullable=false)
	String engine;
	
	// the connection visible database name
	@Column(name="db", nullable=false)
	String db;
	
	// the session id - specific to the server
	@Column(name="session_id",nullable=false)
	int sessionID;
	
	// the server id
	// can be null when not in multijvm mode
	@Column(name="server_id",nullable=false)
	private String server;

	public TemporaryTable() {
		
	}
	
	public TemporaryTable(String tableName, String tableDatabase, String engineName, int connID) {
		this.server = GroupManager.getCoordinationServices().getMemberAddress().toString();
		this.sessionID = connID;
		this.engine = engineName;
		this.name = tableName;
		this.db = tableDatabase;
	}
	
	@Override
	public int getId() {
		return id;
	}

	public String getTableName() {
		return name;
	}

	public String getDatabaseName() {
		return db;
	}

	public String getEngineName() {
		return engine;
	}
	

	public int getSessionID() {
		return sessionID;
	}
	
	public String getServer() {
		return server;
	}
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDrop() {
		// TODO Auto-generated method stub

	}

	private static final GroupMembershipListener tossConnsOnMembershipExit = new GroupMembershipListener() {

		@Override
		public void onMembershipEvent(MembershipEventType eventType,
				InetSocketAddress inetSocketAddress) {
			if (MembershipEventType.MEMBER_REMOVED == eventType &&
					inetSocketAddress.equals(GroupManager.getCoordinationServices().getMemberAddress())) {
				// toss all my connections
				Singletons.require(Host.class).execute(new Runnable() {

					@Override
					public void run() {
						try {
							PerHostConnectionManager.INSTANCE.closeAllConnectionsWithUserlandTemporaryTables();
						} catch (PEException pe) {
							logger.warn("unable to close all conns with temporary tables", pe);
						}
					}
					
				});
			}
		}
		
	};
	
	public static void onStartServices() {
		cleanupCatalog();
		GroupManager.getCoordinationServices().addMembershipListener(tossConnsOnMembershipExit);
	}
	
	public static void onStopServices() {
		GroupManager.getCoordinationServices().removeMembershipListener(tossConnsOnMembershipExit);
		cleanupCatalog();
	}
	
	private static void cleanupCatalog() {
		CatalogDAO c = null;
		try {
			// tried using CatalogDAO.getCatalogDS but the repl slave test seems to not like that
			c = CatalogDAOFactory.newInstance();
			c.begin();
			c.cleanupUserlandTemporaryTables(GroupManager.getCoordinationServices().getMemberAddress().toString());
			c.commit();
		} catch (Throwable t) {
			// should figure out what to do with this
		} finally {
			if (c != null) try {
				c.close();
			} catch (Throwable t) {
				// did our best
			}
		}
	}
	
}
