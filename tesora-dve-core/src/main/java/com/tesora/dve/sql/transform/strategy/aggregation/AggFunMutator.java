// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;

abstract class AggFunMutator extends ColumnMutator {
	
	protected SetQuantifier quantifier;
	protected FunctionName fn;
	
	public boolean isDistinct() {
		return SetQuantifier.DISTINCT == quantifier;
	}
	
	public boolean requiresNoGroupingFirstPass() {
		return false;
	}
	
}