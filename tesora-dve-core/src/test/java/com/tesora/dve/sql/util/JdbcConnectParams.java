package com.tesora.dve.sql.util;

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

import java.util.Properties;

import com.tesora.dve.common.PEUrl;
import com.tesora.dve.exceptions.PEException;

public class JdbcConnectParams {
	private PEUrl peurl;
	private String userid;
	private String password;
	
	JdbcConnectParams(String url, String userid, String password) throws PEException {
		this.peurl = PEUrl.fromUrlString(url);
		this.userid = userid;
		this.password = password;
	}

	public JdbcConnectParams(JdbcConnectParams other) {
		this.peurl = other.peurl;
		this.userid = other.userid;
		this.password = other.password;
	}
	
	public String getUrl() throws PEException {
		return peurl.getURL();
	}
	
	public PEUrl getPEUrl() {
		return peurl;
	}
	
	public void setUrl(String url) throws PEException {
		this.peurl = PEUrl.fromUrlString(url);
	}
	
	public String getUserid() {
		return userid;
	}
	
	public void setUserid(String userid) {
		this.userid = userid;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setURLOptions(Properties urlOptions) throws Throwable {
		peurl.setQueryOptions(urlOptions);
	}
}