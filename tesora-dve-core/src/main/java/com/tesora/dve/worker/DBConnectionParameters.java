// OS_STATUS: public
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

import java.io.Serializable;
import java.util.Properties;

import com.tesora.dve.common.DBHelper;


/**
 * DBConnectionParameter represents the configuration parameters needed to connect
 * to a specific database instance (for example, userid, password, and url).
 */

public class DBConnectionParameters implements Serializable {
	
	private static final long serialVersionUID = -34218052432562134L;
	
	static final private String URL_PROP = "url";
	static final private String USERID_PROP = "user";
	static final private String PASSWORD_PROP = "password";
	
	String jdbcURL;
	String userid;
	String password;
	
	public DBConnectionParameters(DBConnectionParameters dbcp) {
		this.jdbcURL = dbcp.jdbcURL;
		this.userid = dbcp.userid;
		this.password = dbcp.password;
	}
	
	/**
	 * Constructs a {@link DBConnectionParameters} by reading the parameters
	 * as properties from the supplied {@link Properties} object.  The properties read
	 * are the keys "url", "user", and "password" prefixed by the string specified in the 
	 * <b>prefix</b> parameter. 
	 * 
	 * @param props <code>Properties</code> to read values from
	 * @param prefix String to prefix property names with
	 */
	public DBConnectionParameters(Properties props, String prefix) {
		initFromProperties(props, prefix);
	}

	/**
	 * Constructs a {@link DBConnectionParameters} by reading the parameters
	 * from the supplied {@link Properties} object by calling {@link DBConnectionParameters#DBConnectionParameters(Properties, String)}
	 * with the default prefix "javax.persistence.jdbc".
	 * 
	 * @param props <code>Properties</code> to read values from
	 */
	public DBConnectionParameters(Properties props) {
		initFromProperties(props, DBHelper.CONN_ROOT);
	}

	void initFromProperties(Properties props, String prefix) {
		this.jdbcURL = props.getProperty(prefix + URL_PROP);
		this.userid = props.getProperty(prefix + USERID_PROP);
		this.password = props.getProperty(prefix + PASSWORD_PROP);
	}

	/**
	 * Returns the JDBC URL string
	 * @return the JDBC url
	 */
	public String getJdbcURL() {
		return jdbcURL;
	}

	/**
	 * Returns the userid
	 * @return the userid
	 */
	public String getUserid() {
		return userid;
	}

	/**
	 * Returns the password
	 * 
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
}
