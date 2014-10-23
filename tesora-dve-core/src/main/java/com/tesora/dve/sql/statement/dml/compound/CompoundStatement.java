package com.tesora.dve.sql.statement.dml.compound;

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
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public abstract class CompoundStatement extends Statement {

	public CompoundStatement(SourceLocation location) {
		super(location);
		// TODO Auto-generated constructor stub
	}

	public boolean isCompound() {
		return true;
	}
	
	public boolean isDML() {
		return false;
	}
	
	@Override
	public final void plan(SchemaContext sc, ExecutionSequence es,
			BehaviorConfiguration config) throws PEException {
		throw new PEException("Illegal call to CompoundStatement.plan");
	}

	// compound stmts are planned as parts of other stuff (i.e. they are not top level stmts).
	public FeatureStep plan(SchemaContext sc, BehaviorConfiguration config) throws PEException {
		throw new PEException("Not yet implemented.  CompoundStatement.plan for " + getClass().getSimpleName());
	}
	
}
