package com.tesora.dve.common;

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

public interface PEConstants {
	public static final String SERVER_FILE_NAME = "server.properties";
	public static final String CONFIG_FILE_NAME = "dve.properties";
	public static final String SERVER_OVERRIDE_FILE_NAME = "server.override.properties";

	public static final String ROOT = "root";
	public static final String PASSWORD = "password";
	public static final String LOCALHOST = "localhost";
	public static final String JDBC = "jdbc";
	
	public static final String CATALOG = "dve_catalog";

	public static final String MYSQL_PORTAL_PORT_PROPERTY = "MySqlPortal.port";
	public static final String MYSQL_PORTAL_DEFAULT_PORT = "3306";

	public static final String MYSQL_PROTOCOL = JDBC;
	public static final String MYSQL_SUBPROTOCOL = "mysql";
	public static final String MYSQL_HOST = LOCALHOST;
	public static final int MYSQL_PORT = 3306;

	public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";	
	public static final String MARIADB_DRIVER_CLASS = "org.mariadb.jdbc.Driver";	

	public static final String MYSQL_URL = "jdbc:mysql://localhost:3306";
	public static final String MYSQL_URL_3307 = "jdbc:mysql://localhost:3307";
	
	public static final String PROP_JPA_PREFIX = "javax.persistence.";
	public static final String PROP_JPA_JDBC_PREFIX = PROP_JPA_PREFIX + "jdbc.";

	public static final String PROP_JDBC_URL = "jdbc.url";
	public static final String PROP_JDBC_USER = "jdbc.user";
	public static final String PROP_JDBC_PASSWORD = "jdbc.password";
	public static final String PROP_JDBC_DRIVER = "jdbc.driver";
	
	public static final String PROP_DBNAME = "hibernate.default_schema";
	
	public static final String PROP_FULL_JDBC_URL = PROP_JPA_PREFIX + PROP_JDBC_URL;
	public static final String PROP_FULL_JDBC_USER = PROP_JPA_PREFIX + PROP_JDBC_USER;
	public static final String PROP_FULL_JDBC_PASSWORD = PROP_JPA_PREFIX + PROP_JDBC_PASSWORD;
	public static final String PROP_FULL_JDBC_DRIVER = PROP_JPA_PREFIX + PROP_JDBC_DRIVER;
	
	public static final String DVE_SERVER_VERSION = "5.5.10";
	public static final String DVE_SERVER_VERSION_COMMENT = "Tesora (TM) Database Virtualization Engine (R)";
	public static final String DVE_SERVER_COPYRIGHT_COMMENT = "Copyright (c) 2011 - 2014 Tesora. All rights reserved.";
	
	public static final String YES = "yes";
	public static final String NO = "no";
	
	public static final String SYSTEM_GROUP_NAME = "SystemGroup";
	public static final String SYSTEM_SITENAME = "SystemSite";
	public static final String DEFAULT_GROUP_NAME = "DefaultGroup";
	public static final String DEFAULT_DBNAME = "DefaultDB";
	public static final String INFORMATION_SCHEMA_DBNAME = "INFORMATION_SCHEMA";
	public static final String SHOW_SCHEMA_DBNAME = "SHOW_SCHEMA";
	public static final String MYSQL_SCHEMA_DBNAME = "mysql";
	public static final String INFORMATION_SCHEMA_GROUP_NAME = "InformationSchemaGroup";
	public static final String LANDLORD_TENANT = "mtlandlord";
	
	public static final String BOOTSTRAP_PROVIDER_NAME = "BootstrapProvider";
	
	public static final String AGGREGATE = "Aggregate";
	public static final String AGGREGATION = "Aggregation";
	public static final String SMALL = "Small";
	public static final String MEDIUM = "Medium";
	public static final String LARGE = "Large";
	
	public static final String STRICT = "strict";
	
	public static final String DEFAULT_DB_ENGINE = "InnoDB";
	public static final String DEFAULT_TABLE_TYPE = "BASE TABLE";
	
	// keep doing this everywhere - just have one copy
	public static final String LINE_SEPARATOR = System.getProperty("line.separator");
}
