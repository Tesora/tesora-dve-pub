// OS_STATUS: public
package com.tesora.dve.resultset;


import java.io.Serializable;
import java.util.Arrays;

import com.tesora.dve.common.PEStringUtils;

public class ProjectionInfo implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private ColumnInfo[] columns;
	
	public ProjectionInfo(int width) {
		columns = new ColumnInfo[width];
	}
	
	public ColumnInfo addColumn(int index, String name, String alias) {
		columns[index - 1] = new ColumnInfo(name, alias);
		return columns[index - 1];
	}
	
	public ColumnInfo getColumnInfo(int index) {
		return columns[index - 1];
	}
	
	public String getColumnAlias(int index) {
		String alias = columns[index - 1].getAlias();
		if (alias == null)
			alias = columns[index - 1].getName();
		return alias;
	}
	
	public int getWidth() {
		return columns.length;
	}
	
	public void unionize() {
		for(ColumnInfo ci : columns)
			ci.unionize();
	}
	
	@Override
	public String toString() {
		return PEStringUtils.toString(this.getClass(), Arrays.asList(columns));
	}
}
