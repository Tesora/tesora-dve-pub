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
