package com.tesora.dve.sql.statement.session;

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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.PlanningResult;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class ExecutePStmtStatement extends PStmtStatement {

	private List<VariableInstance> vars;
	
	public ExecutePStmtStatement(UnqualifiedName name, List<VariableInstance> vars) {
		super(name);
		this.vars = vars;
	}
	
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		List<Object> values = new ArrayList<Object>();
		for(VariableInstance vi : vars) {
			String v = sc.getConnection().getVariableValue(vi.buildAccessor(sc));
			if (v == null)
				values.add(null);
			else
				values.add(DBTypeBasedUtils.escape(v));
		}
		PlanningResult pr = InvokeParser.bindPreparedStatement(sc, getName().get(), values);
		// this is not the right way to do this at all
		sc.setValues(pr.getValues().getRootValues());
		RootExecutionPlan ep = pr.getPlans().get(0);
		es.setSteps(ep.getSequence().getSteps());
	}
}
