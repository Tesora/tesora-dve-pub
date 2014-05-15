// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.GroupByRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.GroupByRewriteTransformFactory.AliasingEntry;
import com.tesora.dve.sql.util.ListSet;

// responsible for keeping track of the grouping information
public class AggregationMutatorState extends MutatorState {

	protected ListSet<AliasingEntry<SortingSpecification>> normalizedGroupBys;
	protected ListSet<AliasingEntry<SortingSpecification>> originalGroupBys;
	
	public AggregationMutatorState(SchemaContext sc, SelectStatement ss) {
		super(ss);
		normalizedGroupBys = GroupByRewriteTransformFactory.buildEntries(sc, ss, ss.getGroupBysEdge(), MutatorGroupBySortingEntry.builder,
				SortingSpecification.getTarget);
	}
	
	public void clearGroupBys() {
		getStatement().getGroupBysEdge().clear();
	}
	
	public ListSet<AliasingEntry<SortingSpecification>> getRequestedGrouping() {
		return normalizedGroupBys;
	}
}
