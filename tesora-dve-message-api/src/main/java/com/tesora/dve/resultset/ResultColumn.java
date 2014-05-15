// OS_STATUS: public
package com.tesora.dve.resultset;

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
