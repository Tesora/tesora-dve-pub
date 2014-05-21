// OS_STATUS: public
package com.tesora.dve.siteprovider;

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

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.exceptions.PEException;

public abstract class AbstractDynamicSiteInfo {

	protected String provider;
	protected String pool;
	protected String name;
	protected String url;
	protected String user;
	protected String decryptedPassword;
	protected int maxQueries = -1;
	protected DynamicSiteStatus state = DynamicSiteStatus.ONLINE;
	
	protected AtomicInteger totalQueries = new AtomicInteger(0);
	protected AtomicInteger currentQueries = new AtomicInteger(0);
	protected Timestamp timestamp = new Timestamp(System.currentTimeMillis());

	protected HashSet<String> dbNames = new HashSet<String>();
	
	protected AbstractDynamicSiteInfo() {
	}

	public AbstractDynamicSiteInfo(String provider, String pool, String name, String url, String user, String password, int maxQueries) {
		this.provider = provider;
		this.pool = pool;
		this.name = name;
		this.url = url;
		this.user = user;
		this.decryptedPassword = password;
		this.maxQueries = maxQueries;
	}
	
	public AbstractDynamicSiteInfo(AbstractDynamicSiteInfo site) {
		this.provider = site.provider;
		this.pool = site.pool;
		this.name = site.name;
		this.url = site.url;
		this.user = site.user;
		this.decryptedPassword = site.decryptedPassword;
		this.maxQueries = site.maxQueries;
		this.state = site.state;
	}
	
	public String getProvider() {
		return provider;
	}

	public void setProvider(String provider) {
		this.provider = provider;
	}
	
	public String getPool() {
		return pool;
	}

	public void setPool(String pool) {
		this.pool = pool;
	}
	
	public String getFullName() {
		return provider + "_" + pool + "_" + name;
	}

	public String getShortName() {
		return name;
	}

	public void setShortName(String name) {
		this.name = name;
	}

	public DynamicSiteStatus getState() {
		return state;
	}

	public void setState(DynamicSiteStatus state, boolean recordTime) {
		this.state = state;
		timestamp.setTime(System.currentTimeMillis());
	}
	
	public Timestamp getTimestamp() {
		return this.timestamp;
	}
	
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getUser() {
		return user;
	}
	
	public void setUser(String user) {
		this.user = user;
	}

	public String getDecryptedPassword() {
		return decryptedPassword;
	}
	
	public void setDecryptedPassword(String password) {
		this.decryptedPassword = password;
	}

	public int getMaxQueries() {
		return maxQueries;
	}

	public void setMaxQueries(int maxQueries) {
		this.maxQueries = maxQueries;
	}

	public int getCurrentQueries() {
		return currentQueries.get();
	}

	public void setCurrentQueries(int newValue) {
		this.currentQueries.set(newValue);
	}

	public int getTotalQueries() {
		return totalQueries.get();
	}

	public void setTotalQueries(int newValue) {
		this.totalQueries.set(newValue);
	}
	
	public boolean hasCapacity() {
		return (maxQueries == -1) || (currentQueries.get() < maxQueries);
	}
	
	public boolean supportsTransactions() {
		return false;
	}
	
	public synchronized boolean hasDatabase(UserVisibleDatabase ctxDB) {
		return dbNames.contains(ctxDB.getUserVisibleName());
	}

	public synchronized void setHasDatabase(UserVisibleDatabase ctxDB) throws PEException {
		dbNames.add(ctxDB.getUserVisibleName());
	}

}
