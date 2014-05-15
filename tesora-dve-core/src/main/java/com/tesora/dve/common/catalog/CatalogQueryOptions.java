// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.io.Serializable;

public class CatalogQueryOptions implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private boolean extensions;
	private boolean plural;
	private boolean tenant;
	
	public CatalogQueryOptions(boolean exts, boolean many, boolean isTenant) {
		extensions = exts;
		plural = many;
		tenant = isTenant;
	}
	
	public boolean isPlural() {
		return plural;
	}
	
	public boolean emitExtensions() {
		return extensions;
	}
	
	public boolean isTenant() {
		return tenant;
	}
	
}
