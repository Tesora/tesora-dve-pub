// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public interface FeaturePlanner {

	// return the step if we apply, elsewise null
	// note that parent information is stored in the planner context
	public FeatureStep plan(DMLStatement stmt, PlannerContext context) throws PEException;
	
	public FeaturePlannerIdentifier getFeaturePlannerID();

	public boolean emitting();
	
	public void emit(String what);
}
