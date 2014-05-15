// OS_STATUS: public
package com.tesora.dve.persist;

import java.util.List;

import com.tesora.dve.exceptions.PEException;

// we need a persistence layer for installing the info schema that works both when the catalog dao is present
// and when it is not.  we need this because we have to upgrade the info schema from time to time (this is nominally
// a catalog upgrade even though the catalog structure may not change).  the basic things this layer has to do
// is be able to populate an existing catalog (or a new catalog), keeping in mind that autoincs are needed and
// potentially other things.
public interface PersistedEntity {

	public boolean hasGeneratedId();

	// all the entities that must exist before this entity can exist
	public List<PersistedEntity> getRequires();
		
	// the meat of the thing, get the sql that inserts this entity
	public PersistedInsert getInsertStatement() throws PEException;
	
	public long getGeneratedID() throws PEException;
	
	public void onInsert(long genID) throws PEException;
	
}
