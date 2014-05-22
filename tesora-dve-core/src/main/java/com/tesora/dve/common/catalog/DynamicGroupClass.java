package com.tesora.dve.common.catalog;

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
import java.util.StringTokenizer;

import javax.persistence.Embeddable;
import javax.persistence.Transient;

import com.tesora.dve.exceptions.PECodingException;

@Embeddable
public class DynamicGroupClass implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final static String ConfigSepChar = "/";

	@Transient
	String name = null;
	
	String provider;
	// this is actually pool-info
	String className;
	int count;


	public DynamicGroupClass(String name, String Provider, String className, int count) {
		this.name = name;
		this.provider = Provider;
		this.className = className;
		this.count = count;
	}
	
	public DynamicGroupClass(String name, String config) {
		this.name = name;
		if(config != null)
			parseConfigString(config);
	}
	
	public DynamicGroupClass() {
	}

	public String getName() {
		if (name == null)
			throw new PECodingException("DynamicGroupClass name field not set");
		return name;
	}

	public String getProvider() {
		return provider;
	}
	
	public void setProvider(String provider) {
		this.provider = provider;
	}

	public String getPoolName() {
		return className;
	}
	
	public void setPoolName(String className) {
		this.className = className;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	public void parseConfigString(String config) {
		// config is in the same form we use for the toString()
		StringTokenizer st = new StringTokenizer(config, ConfigSepChar);
		
		this.provider = st.nextToken().trim();
		this.className = st.nextToken().trim();
		this.count = Integer.valueOf(st.nextToken().trim());
	}
	
	public String buildConfigString() {
		return provider + ConfigSepChar + className + ConfigSepChar + count;
	}

	@Override
	public String toString() {
		return buildConfigString();
	}
	
	public StringBuffer toString(StringBuffer display) {
		return display.append(toString());
	}
}