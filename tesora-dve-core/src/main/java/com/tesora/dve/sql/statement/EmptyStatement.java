package com.tesora.dve.sql.statement;

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
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class EmptyStatement extends Statement {

	private String text;
	private final StatementType logicalType;
	
	public EmptyStatement(String txt) {
		this(txt,StatementType.UNIMPORTANT);
	}
	
	public EmptyStatement(String txt, StatementType stmtType) {
		super(null);
		text = txt;
		logicalType = stmtType;
	}

	@Override
	public void normalize(SchemaContext sc) {
		// TODO Auto-generated method stub

	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		es.append(new EmptyExecutionStep(0,text));
	}

	@Override
	public StatementType getStatementType() {
		return logicalType;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return illegalSchemaSelf(other);
	}

	@Override
	protected int selfHashCode() {
		return illegalSchemaHash();
	}
	
}
