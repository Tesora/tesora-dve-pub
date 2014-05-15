// OS_STATUS: public
package com.tesora.dve.persist;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;

public class PersistedInsert {

	SimpleTableMetadata table;
	List<SimpleColumnMetadata> columns = new ArrayList<SimpleColumnMetadata>();
	List<Object> values = new ArrayList<Object>();
	
	public PersistedInsert(SimpleTableMetadata tab) {
		table = tab;
	}
	
	public void add(SimpleColumnMetadata column, Object value) {
		columns.add(column);
		values.add(value);
	}
	
	public String getSQL() {
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO `").append(table.getName()).append("` (");
		Functional.join(columns, buf, ", ", new BinaryProcedure<SimpleColumnMetadata,StringBuilder>() {

			@Override
			public void execute(SimpleColumnMetadata aobj, StringBuilder bobj) {
				bobj.append("`").append(aobj.getName()).append("`");
			}
			
		});
		buf.append(") VALUES (");
		Functional.join(values, buf, ", ", new BinaryProcedure<Object,StringBuilder>() {

			@Override
			public void execute(Object aobj, StringBuilder bobj) {
				if (aobj == null)
					bobj.append("NULL");
				else if (aobj instanceof String)
					bobj.append("'").append(aobj).append("'");
				else
					bobj.append(aobj);
			}
			
		});
		buf.append(")");
		return buf.toString();
	}
	
}
