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

import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.behaviors.FeaturePlanTransformerBehavior;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

// eventually this will do the wide result set transform
public final class DefaultPostPlanningFeaturePlanTransformer implements
		FeaturePlanTransformerBehavior {

	public static final FeaturePlanTransformerBehavior INSTANCE = new DefaultPostPlanningFeaturePlanTransformer();
	
	@Override
	public FeatureStep transform(PlannerContext pc, DMLStatement stmt,
			FeatureStep existingPlan) {
		return existingPlan;
	}

}
