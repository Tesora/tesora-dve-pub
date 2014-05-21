// OS_STATUS: public
package com.tesora.dve.server.statistics;

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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class SiteStatKey {
	public enum OperationClass {
		DDL, EXECUTE, EXECUTE_BATCH, FETCH, TXN, REDIST, QUERY, UPDATE, TOTAL;
		
		public static OperationClass fromString(String val) {
			if (val == null || "".equals(val)) 
				throw new IllegalArgumentException("Operation class value cannot be null or empty!");
			
			for (OperationClass sis : values()) {
				if (StringUtils.equalsIgnoreCase(sis.name(), val)) {
					return sis;
				}
			}
			throw new IllegalArgumentException("'" + val + "' is not a valid operation class");			
		}
	}

	public enum SiteType {
		GLOBAL, DYNAMIC, PERSISTENT;
		
		public static SiteType fromString(String val) {
			if (val == null || "".equals(val)) 
				throw new IllegalArgumentException("Site type value cannot be null or empty!");
			
			for (SiteType sis : values()) {
				if (StringUtils.equalsIgnoreCase(sis.name(), val)) {
					return sis;
				}
			}
			throw new IllegalArgumentException("'" + val + "' is not a valid site type");
		}
	}

	private SiteType type;
	private String name;
	private OperationClass opClass;

	public SiteStatKey(SiteType type, String name, OperationClass opClass) {
		this.type = type;
		this.name = name;
		this.opClass = opClass;
	}

	@Override
	public String toString() {
		return type + "." + name + "/" + opClass;
	}

	public SiteType getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public OperationClass getOpClass() {
		return opClass;
	}

	public void setOpClass(OperationClass opClass) {
		this.opClass = opClass;
	}
	
	public int compareTo(SiteStatKey anotherSsk) {
		return new CompareToBuilder()
				.append(this.type, anotherSsk.type)
				.append(this.name, anotherSsk.name)
				.append(this.opClass, anotherSsk.opClass)
				.toComparison();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder()
				.append(this.type)
				.append(this.name)
				.append(this.opClass)
				.toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		SiteStatKey anotherSsk = (SiteStatKey) obj;
		return new EqualsBuilder()
				.append(this.type, anotherSsk.type)
				.append(this.name, anotherSsk.name)
				.append(this.opClass, anotherSsk.opClass)
				.isEquals();
	}

}
