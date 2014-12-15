package com.tesora.dve.charset;

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

import java.nio.charset.Charset;

public class MysqlNativeCharSet extends NativeCharSet {
	
	public static final MysqlNativeCharSet ASCII = new MysqlNativeCharSet(11, "ascii", "US ASCII", 1, Charset.forName("US-ASCII"));
	public static final MysqlNativeCharSet LATIN1 = new MysqlNativeCharSet(8, "latin1", "cp1252 West European", 1, Charset.forName("ISO-8859-1"));
	public static final MysqlNativeCharSet UTF8 = new MysqlNativeCharSet(33, "utf8", "UTF-8 Unicode", 3, Charset.forName("UTF-8"));
	public static final MysqlNativeCharSet UTF8MB4 = new MysqlNativeCharSet(45, "utf8mb4", "UTF-8 Unicode", 4, Charset.forName("UTF-8"));

	public static final MysqlNativeCharSet[] supportedCharSets = new MysqlNativeCharSet[] { ASCII, LATIN1, UTF8, UTF8MB4 };
	
	private static final long serialVersionUID = 1L;

	public MysqlNativeCharSet(int id, String name,
			String description, long maxLen, Charset peCharsetName) {
		super(id, name, description, maxLen, peCharsetName);
	}

}
