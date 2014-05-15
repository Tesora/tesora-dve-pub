// OS_STATUS: public
package com.tesora.dve.charset;

import java.io.Serializable;
import java.nio.charset.Charset;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.StringUtils;

public abstract class NativeCharSet implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String name;
	private String description;
	long maxLen;
	private Charset peCharset; // the corresponding CharsetUtils value
	
	public NativeCharSet(int id, String name, String description, long maxLen, Charset peCharset) {
		this.id = id;
		this.name = name;
		this.description = description;
		this.maxLen = maxLen;
		this.peCharset = peCharset;
	}

	public String getName() {
		return name;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public long getMaxLen() {
		return maxLen;
	}

	public void setMaxLen(long maxLen) {
		this.maxLen = maxLen;
	}

	public Charset getJavaCharset() {
		return peCharset;
	}

	public void setPeCharset(Charset peCharset) {
		this.peCharset = peCharset;
	}

	public boolean isCompatibleWith(final String collation) {
		String charsetName = null;
		try {
			charsetName = Singletons.require(HostService.class).getDBNative().getSupportedCharSets().findCharSetByCollation(collation, false).getName();
		} catch(Exception e) {
			// to prevent adding the throw clause to method definition
		}
		return StringUtils.equalsIgnoreCase(name, charsetName);
	}
}
