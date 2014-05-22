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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.sql.DataSource;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="txn_record")
public class TransactionRecord implements CatalogEntity {
	
//	private static Logger logger = Logger.getLogger(TransactionRecord.class);
	private static final long serialVersionUID = 1L;

	@Id
	String xid;
	
	@Basic(optional=false)
	String host;
	
	@Column(name="is_committed", nullable=false)
	int isCommitted;
	
	@Column(name="created_date", nullable=false)
	java.sql.Timestamp createdDate;
	
	public TransactionRecord() {
	}

	public TransactionRecord(String transId) {
		super();
		this.xid = transId;
		this.host = GroupManager.getCoordinationServices().getMemberAddress().toString();
		this.isCommitted = 0;
	}

	public String getXid() {
		return xid;
	}

	public String getHost() {
		return host;
	}

	public boolean isCommitted() {
		return isCommitted == 1;
	}

	public void setCommitted(boolean isCommitted) {
		this.isCommitted = isCommitted ? 1 : 0;
	}
	
//	public void recover2PC(CatalogDAO c, UserAuthentication userAuth) {
//		System.out.println("In Recovery");
//		try {
//			for (TransactionSite txnSite : txnSiteList) {
//				XADataSource xaDS = Host.getDBNative().getXADataSource(txnSite.getSite().getMasterUrl());
//				XAConnection xaCon= xaDS.getXAConnection(userAuth.getUserid(), userAuth.getPassword());
//				XAResource xaRes = xaCon.getXAResource();
//				Xid xid = new DveXid(getTxnId(), txnSite.getWorkerId());
//				System.out.println("Recovering " + xid);
//				try {
//					if (isCommitted())
//						xaRes.commit(xid, false);
//					else
//						xaRes.rollback(xid);
//				} catch (Exception e) {
//					if (!"XAER_NOTA: Unknown XID".equals(e.getMessage()))
//						throw e;
//					System.out.println("Ignoring: " + e.getLocalizedMessage());
//					e.printStackTrace();
//				} finally {
//					xaCon.close();
//				}
//			}		
//			c.begin();
//			c.remove(this);
//			c.commit();
//			System.out.println("Transaction " + txnId + " recovered");
//		} catch (Exception e) {
//			// If we get an exception part way through, we're screwed!
//			logger.fatal("Exception performing 2PC Recovery of transaction " + txnId + " - FATAL! - system shutting down", e);
//			e.printStackTrace();
//			Runtime.getRuntime().halt(42);
//		}
//	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}
	
	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

	@Override
	public int getId() {
		return 0;
	}

	private static void executeUpdateSQL(DataSource catalogDS, String cmd) throws PEException {
		try {
			Connection c = catalogDS.getConnection();
			try {
				Statement s = c.createStatement();
				if (s.executeUpdate(cmd) != 1)
					throw new PEException("Statement did not execute: " + cmd);
			}
			finally {
				c.close();
			}
		} catch (SQLException e) {
			throw new PEException("Unable to record transaction: " + cmd, e);
		}
	}

	public Boolean recordPrepare(DataSource catalogDS) throws PEException {
		executeUpdateSQL(catalogDS, 
				"INSERT INTO txn_record (xid, host, is_committed, created_date) " 
						+ "VALUES ('" + xid + "', '" + host + "', 0, now())");
		return true;
	}

	public boolean recordCommit(DataSource catalogDS) throws PEException {
		executeUpdateSQL(catalogDS, "UPDATE txn_record SET is_committed = 1 WHERE xid = '" + xid + "'");
		return true;
	}

	public Boolean clearTransactionRecord(DataSource catalogDS) throws PEException {
		executeUpdateSQL(catalogDS, "DELETE FROM txn_record WHERE xid = '" + xid + "'");
		return true;
	}
	
	public static Boolean clearOldTransactionRecords(DataSource catalogDS) throws PEException {
		try {
			Connection c = catalogDS.getConnection();
			try {
				Statement s = c.createStatement();
				s.executeUpdate("DELETE FROM txn_record WHERE datediff(now(),created_date) > 7");
			}
			finally {
				c.close();
			}
		} catch (SQLException e) {
			throw new PEException("Unable to clear transactions", e);
		}
		return true;
	}
	
	public static Map<String, Boolean> getGlobalCommitMap(DataSource catalogDS) throws PEException {
		return getCommitMapByQuery(catalogDS, "SELECT xid, is_committed FROM txn_record");
	}
	
	public static Map<String, Boolean> getCommitMapByHost(DataSource catalogDS, String host) throws PEException {
		return getCommitMapByQuery(catalogDS, "SELECT xid, is_committed FROM txn_record WHERE host = '" + host + "'");
	}
	
	private static Map<String, Boolean> getCommitMapByQuery(DataSource catalogDS, String query) throws PEException {
		Map<String, Boolean> commitMap = new ConcurrentHashMap<String, Boolean>();
		try {
			Connection c = catalogDS.getConnection();
			try {
				Statement s = c.createStatement();
				ResultSet rs = s.executeQuery(query);
				while (rs.next()) {
					String xid = rs.getString(1);
					int is_committed = rs.getInt(2);
					Boolean isCommitted = (is_committed == 0) ? Boolean.FALSE : Boolean.TRUE;
					commitMap.put(xid, isCommitted);
				}
			}
			finally {
				c.close();
			}
		} catch (SQLException e) {
			throw new PEException("Unable to read transaction records: " + query, e);
		}
		return commitMap;
	}

	public static void recoverFailedMember() {
		
	}
}
