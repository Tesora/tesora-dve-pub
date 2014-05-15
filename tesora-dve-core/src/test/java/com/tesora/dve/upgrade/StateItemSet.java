// OS_STATUS: public
package com.tesora.dve.upgrade;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;


public abstract class StateItemSet {

	protected final String kernel;
	
	public StateItemSet(String kern) {
		kernel = kern;
	}
	
	public StateItemSet build(SchemaStateQuery sqs, DBHelper helper, Map<String,String> params) throws Throwable {
		sqs.build(helper, params, this);
		return this;
	}
	
	public abstract void take(List<String> keyCols, List<Object> valueCols);
	
	public abstract void collectDifferences(StateItemSet other, List<String> messages);
	
	static String buildMessage(Object[] values) {
		return Functional.join(Arrays.asList(values), ",", new UnaryFunction<String,Object>() {

			@Override
			public String evaluate(Object object) {
				if (object == null) return "(null)";
				return object.toString();
			}
			
		});
	}

}
