// OS_STATUS: public
package com.tesora.dve.db;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;

public interface ResultChunkProvider {

	public ColumnSet getColumnSet();

	public ResultChunk getResultChunk();
	
	public Object getSingleColumnValue(int rowIndex, int columnIndex) throws PEException;
	
	public boolean hasResults();
	
	public long getLastInsertId();
	
	public long getNumRowsAffected();
}
