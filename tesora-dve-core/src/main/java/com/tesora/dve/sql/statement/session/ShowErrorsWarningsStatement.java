// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.infomessage.Level;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class ShowErrorsWarningsStatement extends SessionStatement {
	
	Level level = null;
	LimitSpecification limit = null;
	
	public ShowErrorsWarningsStatement(String typeTag, 
			LimitSpecification limit) {
		super();
	
		level = Level.findLevel(typeTag);
		this.limit = limit;
	}

	public Level getLevel() {
		return level;
	}
	
	@Override
	public boolean isPassthrough() {
		return false;
	}	
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		es.append(new DDLQueryExecutionStep(level.getSQLName(),pc.getConnection().getMessageManager().buildShow(level)));
	}
	
	@Override
	protected void clearWarnings(SchemaContext pc) {
		// does nothing
	}
}
