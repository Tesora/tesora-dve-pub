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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class SavepointStatement extends SessionStatement {

	private final Name savepointName;
	private final boolean release;
	
	public SavepointStatement(Name n, boolean release) {
		super();
		savepointName = n;
		this.release = release;
	}
	
	public Name getSavepointName() {
		return savepointName;
	}
	
	public boolean isRelease() {
		return release;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		es.append(new EmptyExecutionStep(0,"unsupported execution: " + getSQL(pc)));
	}

	
}
