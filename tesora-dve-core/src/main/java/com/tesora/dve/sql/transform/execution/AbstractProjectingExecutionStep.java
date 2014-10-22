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

import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public abstract class AbstractProjectingExecutionStep extends
		DirectExecutionStep {

	protected Boolean useRowCount = null;
	
	protected AbstractProjectingExecutionStep(SchemaContext sc, 
			Database<?> db, 
			PEStorageGroup storageGroup, 
			DistributionVector vect, 
			DistributionKey distKey,
			DMLStatement command, 
			DMLExplainRecord splain) throws PEException {
		super(db, storageGroup, command.getExecutionType(), vect, distKey, command.getGenericSQL(sc, false, true), command.getDerivedInfo().doSetTimestampVariable(), splain);
	}

	protected AbstractProjectingExecutionStep(Database<?> db, PEStorageGroup storageGroup,  
			DistributionVector vector, DistributionKey distKey, GenericSQLCommand command,
			DMLExplainRecord splain) throws PEException {
		super(db, storageGroup, ExecutionType.SELECT, vector, distKey, command, false, splain);
	}

	
	public String toString() {
		return getSQL(SchemaContext.threadContext.get(),(String)null).getDecoded();
	}		

}
