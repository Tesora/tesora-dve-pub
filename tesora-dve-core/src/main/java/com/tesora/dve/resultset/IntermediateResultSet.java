// OS_STATUS: public
package com.tesora.dve.resultset;


import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

// when result sets are returned adhoc the intermediate
// representation is this class
public class IntermediateResultSet implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ColumnSet columnSet;
	private List<ResultRow> rows;
	
	public IntermediateResultSet(ColumnSet cs, List<ResultRow> rows) {
		columnSet = cs;
		this.rows = rows;
	}
	
	public IntermediateResultSet(ColumnSet cs, ResultRow singleRow) {
		this(cs, Collections.singletonList(singleRow));
	}
	
	// sometimes we need an empty value - usually when no schema context is handy
	@SuppressWarnings("unchecked")
	public IntermediateResultSet() {
		columnSet = new ColumnSet();
		rows = Collections.EMPTY_LIST;
	}
	
	public boolean isEmpty() {
		return this.rows.isEmpty();
	}
	
	public List<ResultRow> getRows() {
		return this.rows;
	}
	
	public ColumnSet getMetadata() {
		return this.columnSet;
	}
}
