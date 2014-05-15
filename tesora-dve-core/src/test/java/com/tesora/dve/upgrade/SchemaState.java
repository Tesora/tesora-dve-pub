// OS_STATUS: public
package com.tesora.dve.upgrade;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.util.Functional;

public class SchemaState {

	private Map<String,TableState> state;
	
	public SchemaState() {
		this.state = new HashMap<String,TableState>();
	}
	
	public void build(DBHelper helper, List<String> tableNames) throws Throwable {
		for(String tn : tableNames) 
			state.put(tn, new TableState(helper, PEConstants.CATALOG, tn));
	}
	
	public String differs(SchemaState other) {
		List<String> messages = new ArrayList<String>();
		for(Map.Entry<String, TableState> me : state.entrySet()) {
			TableState mine = me.getValue();
			TableState yours = other.state.get(me.getKey());
			String diffs = mine.differs(yours);
			if (diffs != null)
				messages.add("On table " + me.getKey() + ": " + diffs);
		}
		if (messages.isEmpty()) return null;
		return Functional.join(messages, System.getProperty("line.separator"));
	}
	
}
