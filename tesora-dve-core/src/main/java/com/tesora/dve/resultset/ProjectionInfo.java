// OS_STATUS: public
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
