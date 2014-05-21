package com.tesora.dve.worker;

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

import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.User;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class UserCredentials implements com.tesora.dve.db.mysql.common.SimpleCredentials {
	
	@Override
    public String getName() {
		return name;
	}

	@Override
    public String getPassword() {
		return password;
	}

	@Override
    public boolean isCleartext() {
		return isCleartext;
	}

	static Logger logger = Logger.getLogger(UserCredentials.class);

	String name;
	String password;
	boolean isCleartext;
	
	public UserCredentials(String name, String password) {
		super();
		this.name = name;
		this.password = password;
		this.isCleartext = true;
	}

	public UserCredentials(String name, String password, boolean isCleartext) {
		super();
		this.name = name;
		this.password = password;
		this.isCleartext = isCleartext;
	}

	public User authenticate(SSConnection ssConn) throws PEException { 
		// get the appropriate User object from the catalog
		List<User> candidates = ssConn.getCatalogDAO().findUsers(name, null);
				
		if ( candidates.isEmpty() ) {
			throw new PEException("Connection refused - User '" + name + "' not found");
		}

		User catalogUser = null;
		catalogUser = candidates.get(0);
		if (candidates.size() > 1) 
			logger.debug("More than one user found, choosing '" + catalogUser.getName() + "'@'" + catalogUser.getAccessSpec() + "'");
		
		// if the password isn't plain, hash it
		String passwordForAuth;
		if ( isCleartext) {
			passwordForAuth = catalogUser.getPlaintextPassword();
		} else {
			try {
				passwordForAuth = ssConn.getDBNative().getPasswordForAuth(catalogUser, ssConn);
			} catch (Exception e) {
				throw new PEException("Exception occurred while validating password", e);
			}
		}
		
		// compare the passed in password to one from Catalog
		if ( !passwordForAuth.equals(password) ) {
			throw new PEException("Connection refused - Authentication failed for user '" + name + "'");
		}

		return catalogUser;
	}
	

}
