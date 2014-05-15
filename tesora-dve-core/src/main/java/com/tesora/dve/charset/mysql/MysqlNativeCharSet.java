// OS_STATUS: public
package com.tesora.dve.charset.mysql;

import java.nio.charset.Charset;

import com.tesora.dve.charset.NativeCharSet;

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
