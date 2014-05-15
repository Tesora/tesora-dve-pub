// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.transform.strategy.GroupByRewriteTransformFactory.GroupBySortingEntry;
import com.tesora.dve.sql.util.UnaryFunction;

public class MutatorGroupBySortingEntry extends GroupBySortingEntry {

	public MutatorGroupBySortingEntry(SortingSpecification ss) {
		super(ss);
	}

	public static final UnaryFunction<MutatorGroupBySortingEntry, SortingSpecification> builder = 
		new UnaryFunction<MutatorGroupBySortingEntry, SortingSpecification>() {

			@Override
			public MutatorGroupBySortingEntry evaluate(SortingSpecification object) {
				return new MutatorGroupBySortingEntry(object);
			}
		
	};
}
