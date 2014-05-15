// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.transform.AggFunCollector;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;

class ColumnCharacterization {
	
	protected ExpressionAlias entry;
	protected ListSet<ColumnInstance> usedColumns;
	protected ListSet<FunctionCall> aggFuns;
	protected ListSet<ColumnInstance> nonAggFunColumns;
	
	
	public ColumnCharacterization(ExpressionAlias ea) {
		entry = ea;
		aggFuns = AggFunCollector.collectAggFuns(entry);
		usedColumns = ColumnInstanceCollector.getColumnInstances(ea);
		nonAggFunColumns = new ListSet<ColumnInstance>();
		if (aggFuns.isEmpty())
			nonAggFunColumns.addAll(usedColumns);
		else {
			// we're mixed if there is any column which eventually reaches ea without hitting an agg fun
			for(ColumnInstance ci : usedColumns) {
				LanguageNode p = ci;
				boolean any = false;
				while(!(p instanceof ExpressionAlias) && !any && p != null) {
					if (aggFuns.contains(p))
						any = true;
					else
						p = p.getParent();
				}
				if (!any)
					nonAggFunColumns.add(ci);
			}
		}
	}

	public ExpressionAlias getEntry() {
		return entry;
	}
	
	public ListSet<FunctionCall> getAggFuns() {
		return aggFuns;
	}
	
	public boolean hasAnyAggFuns() {
		return !getAggFuns().isEmpty();
	}
	
	public boolean hasAnyAggFunsNoCount() {
		if (getAggFuns() != null) {
			for(FunctionCall fc : getAggFuns()) {
				if (fc.getFunctionName().isAggregateNotIncludingCount()) {
					return true;
				}
			}
		}
		return false;
	}

	public boolean hasAnyNonAggFunColumns() {
		return !nonAggFunColumns.isEmpty();
	}			
}