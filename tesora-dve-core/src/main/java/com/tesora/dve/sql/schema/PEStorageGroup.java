// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;

public interface PEStorageGroup {

	StorageGroup getPersistent(SchemaContext sc);

	PersistentGroup persistTree(SchemaContext sc) throws PEException;

	PEPersistentGroup anySite(SchemaContext sc) throws PEException;

	boolean comparable(SchemaContext sc, PEStorageGroup storage);
	
	boolean isSubsetOf(SchemaContext sc, PEStorageGroup storage);

	boolean isSingleSiteGroup();

	void setCost(int score) throws PEException;
	
	boolean isTempGroup();	
	
	PEStorageGroup getPEStorageGroup(SchemaContext sc);
	
	StorageGroup getScheduledGroup(SchemaContext sc);
	
}
