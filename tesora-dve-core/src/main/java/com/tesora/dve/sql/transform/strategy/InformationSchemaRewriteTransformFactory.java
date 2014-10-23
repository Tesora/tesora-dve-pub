package com.tesora.dve.sql.transform.strategy;

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
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListSet;

public class InformationSchemaRewriteTransformFactory extends TransformFactory {

	public static boolean applies(SchemaContext sc, DMLStatement stmt, boolean hasParent) throws PEException {
		ListSet<TableKey> tables = EngineConstant.TABLES_INC_NESTED.getValue(stmt,sc);
		// so, this transform applies only when any of the tables is an info schema table
		// we throw exceptions for the following causes:
		// mixed info schema and info schema
		// stmt is not a select stmt
		boolean haveInfo = false;
		boolean haveUser = false;
		for(TableKey tk : tables) {
			if (tk.getTable().isInfoSchema())
				haveInfo = true;
			else
				haveUser = true;
		}
		if (!haveInfo) return false;
		if (haveInfo && haveUser)
			throw new PEException("Invalid info schema query: mixes info schema tables and user tables");
		if (!(stmt instanceof SelectStatement))
			throw new PEException("Invalid info schema query: not a select statement");
		if (hasParent)
			throw new PEException("No support for nested info schema queries");
		
		return true;
		
	}
	
	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.INFO_SCHEMA;
	}

	@Override
	public FeatureStep plan(final DMLStatement stmt, PlannerContext ipc) throws PEException {
		if (!applies(ipc.getContext(), stmt, !ipc.getApplied().isEmpty()))
			return null;		
		FeatureStep root = LogicalSchemaQueryEngine.execute(ipc.getContext(), (SelectStatement)stmt, this);
		return root;
	}
}
