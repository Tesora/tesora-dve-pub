// OS_STATUS: public
package com.tesora.dve.db;

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

public class NativeTypeAlias implements Serializable {
	private static final long serialVersionUID = 1L;

	protected String aliasName;
	protected Boolean isJdbcType;

	public NativeTypeAlias() {
	}

	public NativeTypeAlias(String aliasName, Boolean isJdbcType) {
		this.aliasName = aliasName;
		this.isJdbcType = isJdbcType;
	}

	public NativeTypeAlias(String aliasName) {
		this.aliasName = aliasName;
		this.isJdbcType = false;
	}

	public String getAliasName() {
		return aliasName;
	}

	public Boolean isJdbcType() {
		return isJdbcType;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}

	public void setIsJdbcType(Boolean isJdbcType) {
		this.isJdbcType = isJdbcType;
	}
	
	@Override
	public String toString() {
		return aliasName + ":" + isJdbcType;
	}
}
