// OS_STATUS: public
package com.tesora.dve.resultset;

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
