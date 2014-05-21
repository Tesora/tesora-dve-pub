// OS_STATUS: public
package com.tesora.dve.upgrade;

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
