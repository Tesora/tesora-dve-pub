// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.io.Serializable;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

public interface CatalogEntity extends Serializable {
	
	int getId();

	// return a ColumnSet that represents the metadata to display during "SHOW" commands
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) throws PEException;
	
	// return a ResultRow that represents the data to display during "SHOW" commands
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) throws PEException;

	/** 
	 * Generic call to remove the parent references to this entity.  Not doing so causes
	 * a "Deleted entity passed to persist" exception when remove() is called.
	 */
	public void removeFromParent() throws Throwable;

	/** 
	 * Generic call to gather all dependent entities.
	 * Can be used for catalog deletions for example.  
	 */
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable;
	
	void onUpdate();
	void onDrop();
}
