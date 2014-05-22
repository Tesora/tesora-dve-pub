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

public class ResultColumn implements Serializable {

	private static final long serialVersionUID = 1L;

	protected Object columnValue;
	protected boolean isNull = false;
	
	public ResultColumn() {};
	
	public ResultColumn( Object value, boolean isNull ) {
		this.columnValue = value;
		this.isNull = isNull;
	}
	
	public ResultColumn( Object value ) {
		if ( value == null )
			isNull = true;
		
		columnValue = value;
	}

	// copy constructor
	public ResultColumn(ResultColumn rc) {
		this.columnValue = rc.columnValue;
		this.isNull = rc.isNull;
	};
	
	public Object getColumnValue() {
		return columnValue;
	}
	public void setColumnValue(Object columnValue) {
		this.columnValue = columnValue;
	}
	
	public boolean isNull() {
		return isNull;
	}
	
	public void isNull(boolean isNull) {
		this.isNull = isNull;
	}

	public void setNull(boolean isNull) {
		this.isNull( isNull );
	}
	
	@Override
	public String toString() {
		return isNull() ? "NULL" : getColumnValue().toString();
	}
}
