// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

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

import com.tesora.dve.db.mysql.common.MysqlHandshake;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.RandomStringUtils;

import com.tesora.dve.db.mysql.MysqlNativeConstants;

public class SSConnectionHandshake implements MysqlHandshake {

	// TODO - this is Mysql specific - should be generalized
	
	// for ServerGreetingResponse in PEMyProtocolConverter
	private static final String MYSQL_PLUGIN_DATA = "mysql_native_password";

    private int connectionID;
	private String salt;
	
	SSConnectionHandshake(int id) {
		this.salt = generateSalt();
        this.connectionID = id;
	}
	
	String generateSalt() {
		return RandomStringUtils.randomAscii(20);
	}

    @Override
    public int getConnectionId() {
        return this.connectionID;
    }

    @Override
    public String getSalt() {
		return salt;
	}
	@Override
    public String getPluginData() {
		return MYSQL_PLUGIN_DATA;
	}

	@Override
    public byte getServerCharSet() {
		return MysqlNativeConstants.MYSQL_CHARSET_LATIN1;
	}
	
	@Override
    public String getServerVersion() {
        return Singletons.require(HostService.class).getServerVersion();
	}
	
	@Override
    public long getServerCapabilities() {
		return ClientCapabilities.CLIENT_LONG_PASSWORD + ClientCapabilities.CLIENT_FOUND_ROWS + ClientCapabilities.CLIENT_LONG_FLAG
				+ ClientCapabilities.CLIENT_CONNECT_WITH_DB + ClientCapabilities.CLIENT_NO_SCHEMA + ClientCapabilities.CLIENT_ODBC
				+ ClientCapabilities.CLIENT_LOCAL_FILES + ClientCapabilities.CLIENT_IGNORE_SPACE + ClientCapabilities.CLIENT_PROTOCOL_41
				+ ClientCapabilities.CLIENT_INTERACTIVE + ClientCapabilities.CLIENT_IGNORE_SIGPIPE + ClientCapabilities.CLIENT_TRANSACTIONS
				+ ClientCapabilities.CLIENT_RESERVED + ClientCapabilities.CLIENT_SECURE_CONNECTION + ClientCapabilities.CLIENT_MULTI_STATEMENTS
				+ ClientCapabilities.CLIENT_MULTI_RESULTS + ClientCapabilities.CLIENT_PS_MULTI_RESULTS + ClientCapabilities.CLIENT_PLUGIN_AUTH
				+ ClientCapabilities.CLIENT_REMEMBER_OPTIONS;

	}
}
