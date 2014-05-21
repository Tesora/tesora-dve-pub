// OS_STATUS: public
package com.tesora.dve.sql.parser;

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

import java.util.regex.Pattern;

public enum MysqlLogFileEntryKind {

	// this is all the commands we have to do something with	
	QUERY("Query",false),
	CONNECT("Connect",false),
	INITDB("Init DB",false),
	// we may need this in the future
	PREPARE("Prepare",true),
	EXECUTE("Execute",true),
	
	// and all the ones we don't
	SLEEP("Sleep",true),
	QUIT("Quit",false),
	FIELD_LIST("Field List",true),
	CREATE_DB("Create DB",true),
	DROP_DB("Drop DB",true),
	REFRESH("Refresh",true),
	SHUTDOWN("Shutdown",true),
	STATISTICS("Statistics",true),
	PROCESSLIST("Processlist",true),
	KILL("Kill",true),
	DEBUG("Debug",true),
	PING("Ping",true),
	TIME("Time",true),
	DELAYED_INSERT("Delayed insert",true),
	CHANGE_USER("Change user",true),
	BINLOG_DUMP("Binlog Dump",true),
	TABLE_DUMP("Table Dump",true),
	CONNECT_OUT("Connect Out",true),
	REGISTER_SLAVE("Register Slave",true),
	LONG_DATA("Long Data",true),
	CLOSE_STMT("Close stmt",true),
	RESET_STMT("Reset stmt",true),
	SET_OPTION("Set option",true),
	FETCH("Fetch",true),
	DAEMON("Daemon",true),
	ERROR("Error",true),
	BINLOG("Binlog",true);
	
	private final String match;
	private final boolean payload;
	private final boolean omit;
	private final Pattern regex;
	
	private MysqlLogFileEntryKind(String matchKern, boolean ignore) {
		this(matchKern,!ignore,ignore);
	}
	
	private MysqlLogFileEntryKind(String matchkern, boolean pl, boolean ignore) {
		match = matchkern;
		payload = pl;
		String ws = "\\s+";
		String integral = "\\d+";
		String me = "(" + match + ")";
		String pattern = 
			ws + "(" + integral + ws + integral + ":" + integral + ":" + integral + ws + ")?"
			+ "(" + integral + ")" + ws + me + ws;
		regex = Pattern.compile(pattern);
		omit = ignore;
	}
	
	public String getMatch() { return match; }
	public boolean usePayload() { return payload; }
	public Pattern getRegex() { return regex; }
	public boolean ignore() { return omit; }

}
