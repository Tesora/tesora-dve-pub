// OS_STATUS: public
package com.tesora.dve.common.catalog;

public interface PersistentColumn {

	public String getPersistentName();
	
	public String getAliasName();
	
	public int getId();
	
	public String getNativeTypeName();
	
	public int getHashPosition();
	
	
}
