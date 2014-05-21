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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.Types;
import java.util.Map;

import org.junit.Test;

public class ColumnSetTest {

	@Test
	public void basicTest() {

		ColumnSet cs = new ColumnSet();
		ColumnMetadata cm = new ColumnMetadata("col1", Types.INTEGER, "integer");
		cm.setSize(10);
		cm.setAliasName("intcol1");
		cs.addColumn(cm);
		cs.addColumn("col2", 10, "varchar", Types.VARCHAR);
		cs.addColumn("col3", 10, "float", Types.FLOAT, 10, 4);
		cs.addColumn("blob1", 500, "blob", Types.LONGVARBINARY);
		assertEquals(4, cs.size());
		assertEquals(30, cs.calculateRowSize());
		assertEquals(4, cs.getColumnList().size());
	
		ColumnSet newCS = new ColumnSet(cs);
		assertEquals(4, newCS.size());
		
		Map<String, Integer> newCSmap = newCS.getColumnMap(true);
		assertNull(newCSmap.get("col1")); 	// check that the column name that was aliased isn't in the map
		int i = 0;
		for ( ColumnMetadata cmd : cs.getColumnList() ) {
			assertEquals(i++, newCSmap.get(cmd.getQueryName()).intValue());
		}
	}

}
