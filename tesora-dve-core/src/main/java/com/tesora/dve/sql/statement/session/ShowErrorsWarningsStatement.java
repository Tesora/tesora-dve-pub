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
import com.tesora.dve.infomessage.Level;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
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
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		es.append(new DDLQueryExecutionStep(level.getSQLName(),pc.getConnection().getMessageManager().buildShow(level)));
	}
	
	@Override
	protected void clearWarnings(SchemaContext pc) {
		// does nothing
	}
}
