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

import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public abstract class NonDMLFeatureStep extends FeatureStep {

	public NonDMLFeatureStep(FeaturePlanner planner, PEStorageGroup srcGroup) {
		super(planner, srcGroup, null);
		withDefangInvariants();
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return null;
	}

	@Override
	public boolean isDML() {
		return false;
	}
	
	@Override
	public final Database<?> getDatabase(PlannerContext pc) {
		return null;
	}
	
}
