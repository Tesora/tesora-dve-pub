// OS_STATUS: public
package com.tesora.dve.common.catalog;

import com.tesora.dve.distribution.KeyValue;

public interface PersistentTable {

	public String displayName();

	public String getNameAsIdentifier();

	public String getPersistentName();

	public String getQualifiedName();

	public int getNumberOfColumns();
	
	public KeyValue getDistValue();

	public StorageGroup getPersistentGroup();
	
	public DistributionModel getDistributionModel();
	
	public int getId();
	
	public PersistentContainer getContainer();
	
	public PersistentDatabase getDatabase();
	
	public PersistentColumn getUserColumn(String name);	
}
