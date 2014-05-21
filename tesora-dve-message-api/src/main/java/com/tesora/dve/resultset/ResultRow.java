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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.PEStringUtils;

public class ResultRow implements Serializable {

	private static final long serialVersionUID = 1L;

	protected List<ResultColumn> row = new ArrayList<ResultColumn>();

	public ResultRow() {
	}

	// Copy Constructor
	public ResultRow( ResultRow rr ) {
		for ( ResultColumn rc : rr.row ) {
			row.add(new ResultColumn(rc));
		}
	}

	public ResultRow(List<ResultColumn> cols) {
		setRow(cols);
	}

	// add a ResultColumn to the ResultRow that represents a null value
	public ResultRow addResultColumn() {
		this.addResultColumn(null, true);
		return this;
	}

	// add a ResultColumn to the ResultRow that could be null or not null
	public ResultRow addResultColumn(Object value) {
		if ( value == null )
			this.addResultColumn(value, true);
		else
			this.addResultColumn(value, false);
		
		return this;
	}
	
	public ResultRow addResultColumn(Object value, boolean isNull) {
		ResultColumn rc = new ResultColumn(value, isNull);
		this.addResultColumn(rc);
		return this;
	}

	public ResultRow addResultColumn(ResultColumn rc) {
		row.add(rc);
		return this;
	}

	public ResultColumn getResultColumn(Integer index) {
		return row.get(index - 1);
	}

	public List<ResultColumn> getRow() {
		return row;
	}

	public void setRow(List<ResultColumn> row) {
		this.row = row;
	}
	
	@Override
	public String toString() {
		return PEStringUtils.toString(this.getClass(), getRow());
	}

	public static List<ResultRow> singleRow(String value) {
		ResultColumn rc = new ResultColumn(value);
		ResultRow rr = new ResultRow(Collections.singletonList(rc));
		return Collections.singletonList(rr);
	}

}
