// OS_STATUS: public
package com.tesora.dve.sql.schema.mt;

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
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.dbc.ServerDBConnection;
import com.tesora.dve.sql.util.Pair;

public class TableGarbageCollector extends Thread {

	private static final Logger logger = Logger.getLogger( TableGarbageCollector.class );


	private AtomicInteger interval;
	private AtomicInteger request;
	
	private ServerDBConnection persistentConnection;
	
	public TableGarbageCollector(int cleanupInterval) {
		super("AdaptiveMTGarbageCollector");
		setDaemon(true);
		persistentConnection = null;
		request = new AtomicInteger(1);
		interval = new AtomicInteger(cleanupInterval);
	}

	public void setCollectionInterval(int value) {
		interval.set(value);
	}
	
	public void onGarbageEvent() {
		request.incrementAndGet();
	}
	
	private ServerDBConnection getConnection()  throws Throwable {
		if (persistentConnection == null) {
			CatalogDAO c = null;
			String rootName, rootPass;
			try {
				c = CatalogDAOFactory.newInstance();

				User rootUser = c.findDefaultProject().getRootUser();
				rootName = rootUser.getName();
				rootPass = rootUser.getPlaintextPassword();
			} finally {
				c.close();
				c = null;
			}
			persistentConnection = new ServerDBConnection(rootName,rootPass);
			persistentConnection.execute("set foreign_key_checks=0");
		}
		return persistentConnection;
	}
	
	private void closeConnection() throws Throwable {
		if (persistentConnection != null)
			persistentConnection.closeComms();
		persistentConnection = null;
	}
	
	private Pair<Integer,LinkedHashSet<String>> findCandidates(ServerDBConnection conn) throws Throwable {
		LinkedHashSet<String> matching = new LinkedHashSet<String>();
		ResultSet rs = null;
		try {
			rs = conn.executeQuery("select table_name from information_schema.scopes where scope_name is null");
			while(rs.next()) {
				matching.add(rs.getString(1));
			}
		} finally {
			if (rs != null)
				rs.close();
		}
		if (matching.isEmpty()) { 
			return new Pair<Integer,LinkedHashSet<String>>(0,matching);
		}
		int raw = matching.size();
		StringBuilder buf = new StringBuilder();
		buf.append("select referenced_table_name from information_schema.referential_constraints where referenced_table_name in (");
		boolean first = true;
		for(String tn : matching) {
			if (first) first = false;
			else buf.append(",");
			buf.append("'").append(tn).append("'");
		}
		buf.append(")");
		try {
			rs = conn.executeQuery(buf.toString());
			while(rs.next()) {
				matching.remove(rs.getString(1));
			}
		} finally {
			if (rs != null)
				rs.close();
		}
		return new Pair<Integer,LinkedHashSet<String>>(raw,matching);
	}
	
	private void doCleanup() throws Throwable {
		if (request.get() < 1) return;
		String dropSQL = "DROP TABLE IF EXISTS %s";
		
		// get all the table names
		Pair<Integer,LinkedHashSet<String>> matching = findCandidates(getConnection());
		if (matching.getFirst() == 0) {
			// truly nothing to do
			request.decrementAndGet();
			return;
		}
		if (matching.getSecond().isEmpty()) {
			// nothing to do at this time (existing garbage is referred to, cannot be dropped)
			return;
		}
		try {
			getConnection().execute("use " + PEConstants.LANDLORD_TENANT);
		} catch (Throwable t) {
			// landlord doesn't exist - we're done here
			return;
		}
		for(String tn : matching.getSecond()) {
			try {
				// verify that the table is not the target of any fks
				String sql = String.format(dropSQL,tn);
				getConnection().execute(sql);
				logger.warn("Garbage collected table " + tn);
			} catch (Throwable t) {
				logger.warn("Unable to garbage collect table " + tn,t);
				// start fresh
				closeConnection();
			}				
		}
		
		request.decrementAndGet();
	}
	
	
	@Override
	public void run() {
		logger.info("Starting TableCleanup thread with interval=" + interval.get());

		boolean running = true;
		while (running) {
			try {
				sleep(interval.get());
				doCleanup();
				if ( Thread.currentThread().isInterrupted() )
					running = false;
			} catch (InterruptedException e) {
				running = false;
			} catch (Throwable t) {
				logger.error("Exception returned from cleanup",t);
			}
			
			if ( !running )
				logger.info("Stopping TableCleanup thread");
		}

		// we're leaving, close the connection if need be.
		try {
			closeConnection();
		} catch (Throwable t) {
			logger.error("Unable to shut down table garbage collector connection",t);
		}
		
	}
	
}
