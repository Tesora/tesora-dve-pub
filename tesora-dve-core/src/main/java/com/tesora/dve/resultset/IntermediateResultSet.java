package com.tesora.dve.resultset;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */


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
