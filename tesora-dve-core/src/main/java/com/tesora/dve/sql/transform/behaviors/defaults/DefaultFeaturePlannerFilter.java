package com.tesora.dve.sql.transform.behaviors.defaults;

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


import com.tesora.dve.sql.transform.behaviors.FeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public final class DefaultFeaturePlannerFilter implements FeaturePlannerFilter {

	public static final FeaturePlannerFilter INSTANCE = new DefaultFeaturePlannerFilter();
	
	public boolean canApply(PlannerContext pc, FeaturePlannerIdentifier id) {
		return !pc.getApplied().contains(id);
	}
	
}
