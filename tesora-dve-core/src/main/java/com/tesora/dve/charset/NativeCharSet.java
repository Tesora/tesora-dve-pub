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

import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.singleton.Singletons;

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
		final String charsetName = Singletons.require(NativeCharSetCatalog.class).findCharSetByCollation(collation)
				.getName();
		return StringUtils.equalsIgnoreCase(name, charsetName);
	}

	@Override
	public String toString() {
		return this.name;
	}
}
