// OS_STATUS: public
package com.tesora.dve.upgrade;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.DBHelper;

public class SchemaStateQuery {

	// command to execute
	// param sub by :name
	private String command;
	// column that has the key value
	private LinkedHashSet<Integer> keyColumn;
	// columns to ignore
	Set<Integer> ignoreColumns;
	
	public SchemaStateQuery(String command, int keyColumn) {
		this(command,new int[] { keyColumn },null);
	}
							
	@SuppressWarnings("unchecked")
	public SchemaStateQuery(String command, int[] keyColumns, int[] ignoredColumns) {
		
		this.command = command;
		this.keyColumn = new LinkedHashSet<Integer>();
		for(int kc : keyColumns)
			keyColumn.add(kc);
		if (ignoredColumns == null)
			this.ignoreColumns = Collections.EMPTY_SET;
		else {
			this.ignoreColumns = new HashSet<Integer>();
			for(int i = 0; i < ignoredColumns.length; i++)
				this.ignoreColumns.add(ignoredColumns[i]);
		}		
	}
	
	public void build(DBHelper helper, Map<String,String> params, StateItemSet target) throws Throwable {
		String working = command;
		for(Map.Entry<String, String> me : params.entrySet()) {
			working = working.replaceAll(me.getKey(), me.getValue());
		}
		ResultSet rs = null;
		try {
			helper.executeQuery(working);
			rs = helper.getResultSet();
			int ncols = rs.getMetaData().getColumnCount();
			while(rs.next()) {
				List<String> keyCols = new ArrayList<String>();
				for(int i : keyColumn)
					keyCols.add(rs.getString(i));
				List<Object> values = new ArrayList<Object>();
				for(int i = 1; i <= ncols; i++) {
					if (ignoreColumns.contains(i) || keyColumn.contains(i)) continue;
					values.add(rs.getObject(i));
				}
				target.take(keyCols, values);
			}
		} finally {
			if (rs != null)
				rs.close();
		}
	}
	
}
