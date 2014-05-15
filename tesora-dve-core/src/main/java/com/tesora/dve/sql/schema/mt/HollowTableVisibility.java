// OS_STATUS: public
package com.tesora.dve.sql.schema.mt;

public class HollowTableVisibility {

	private String localName;
	private int scopeID;
	private String tenantName;
	private int tenantID;
	private String dbName;
	private int dbid;
	private String tabName;
	private Integer autoIncrementId;
	
	public HollowTableVisibility() {
		
	}

	public String getLocalName() {
		return localName;
	}

	public void setLocalName(String localName) {
		this.localName = localName;
	}

	public String getTenantName() {
		return tenantName;
	}

	public void setTenantName(String tenantName) {
		this.tenantName = tenantName;
	}

	public int getTenantID() {
		return tenantID;
	}

	public void setTenantID(int tenantID) {
		this.tenantID = tenantID;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public int getDbid() {
		return dbid;
	}

	public void setDbid(int dbid) {
		this.dbid = dbid;
	}

	public String getTabName() {
		return tabName;
	}

	public void setTabName(String tabName) {
		this.tabName = tabName;
	}

	public Integer getAutoIncrementId() {
		return autoIncrementId;
	}

	public void setAutoIncrementId(Integer autoIncrementId) {
		this.autoIncrementId = autoIncrementId;
	}
	
	public void setScopeId(int id) {
		this.scopeID = id;
	}
	
	public int getScopeID() {
		return this.scopeID;
	}
	
}
