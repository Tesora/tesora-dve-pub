// OS_STATUS: public
package com.tesora.dve.common.catalog;

public interface HasAutoIncrementTracker {

	public boolean hasAutoIncr();

	public long getNextIncrValue(CatalogDAO c);
	
	public long getNextIncrBlock(CatalogDAO c, long blockSize);

	public void removeNextIncrValue(CatalogDAO c, long value);


}
