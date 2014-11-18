package com.tesora.dve.sql.transform.execution;

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

import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.sql.schema.SchemaContext;

public class ExplainExecutionStep extends DDLQueryExecutionStep {

	private final ExecutionPlan target;
	
	public ExplainExecutionStep(String tag, ExecutionPlan targ, IntermediateResultSet results) {
		super(tag, results);
		this.target = targ;
	}

	@Override
	public void display(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containing, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc,cv,containing,buf,indent,opts);
		target.display(sc,cv,containing,buf,indent + "  ",opts);
	}

	
}
