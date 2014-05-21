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

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public abstract class NonQueryFeatureStep extends FeatureStep {

	private final DMLStatement statement;
	private final TableKey table;
	

	public NonQueryFeatureStep(FeaturePlanner planner, DMLStatement statement, TableKey tableKey, PEStorageGroup srcGroup, DistributionKey key) {
		super(planner, srcGroup, key);
		this.statement = statement;
		this.table = tableKey;
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return statement;
	}

	public TableKey getTable() {
		return table;
	}
	
	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return table.getAbstractTable().getDatabase(pc.getContext());
	}
}
