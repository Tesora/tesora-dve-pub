// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

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
