// OS_STATUS: public
package com.tesora.dve.db;

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
