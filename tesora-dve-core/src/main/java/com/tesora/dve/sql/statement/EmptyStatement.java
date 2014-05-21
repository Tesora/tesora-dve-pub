// OS_STATUS: public
package com.tesora.dve.sql.statement;

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
