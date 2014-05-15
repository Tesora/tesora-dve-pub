// OS_STATUS: public
package com.tesora.dve.common.catalog;

public interface ITenant extends CatalogEntity {

	// tenants have an id - this is used for distribution purposes
	@Override
	public int getId();
	
	// tenants also have an identifier
	public String getUniqueIdentifier();
	
	// is this the global tenant
	public boolean isGlobalTenant();
	
	public boolean isPersistent();
	
}
