// OS_STATUS: public
package com.tesora.dve.common.catalog;

import com.tesora.dve.common.UserVisibleDatabase;

public interface PersistentDatabase extends UserVisibleDatabase {

	public String getNameOnSite(StorageSite site);

	public int getId();

	public String getDefaultCollationName();
	
	public String getDefaultCharacterSetName();

}
