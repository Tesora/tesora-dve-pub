// OS_STATUS: public
package com.tesora.dve.sql.transform;

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


import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.util.ListSet;

public final class AggFunCollector extends GeneralCollectingTraversal {

	private AggFunCollector() {
		super(Order.POSTORDER,ExecStyle.ONCE);
	}
	
	public static ListSet<FunctionCall> collectAggFuns(LanguageNode in) {
		return collect(in,new AggFunCollector());
	}
	
	public static ListSet<FunctionCall> collectAggFuns(Edge<?,?> in) {
		return cast(collect(in,new AggFunCollector()));
	}
	
	@Override
	public boolean is(LanguageNode iln) {
		if (EngineConstant.FUNCTION.has(iln)) {
			FunctionCall fc = (FunctionCall) iln;
			if (fc.getFunctionName().isAggregate()) {
				if (fc.getFunctionName().isUnsupportedAggregate())
					throw new SchemaException(Pass.PLANNER, "Unsupported aggregation function: " + fc.getFunctionName().getSQL());
				return true;
			}
		}
		return false;
	}

}
