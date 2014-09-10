package com.tesora.dve.sql.infoschema.direct;

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

import java.util.HashSet;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.statement.ddl.DDLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public class DirectInfoSchemaStatement extends DDLStatement {

	private final FeatureStep step;
	
	public DirectInfoSchemaStatement(FeatureStep step) {
		super(true);
		this.step = step;
	}

	@Override
	public Action getAction() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		
		step.schedule(new PlannerContext(sc,sc.getBehaviorConfiguration()), es, new HashSet<FeatureStep>());
		

	}
	
	@Override
	protected void preplan(SchemaContext pc, ExecutionSequence es,boolean explain) throws PEException {
	}
	
	public SelectStatement getCatalogQuery() {
		return (SelectStatement) step.getPlannedStatement();
	}
}
