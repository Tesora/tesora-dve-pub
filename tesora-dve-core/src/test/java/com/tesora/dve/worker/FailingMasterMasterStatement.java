package com.tesora.dve.worker;

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

import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.MasterMasterStatement;
import com.tesora.dve.worker.Worker;

public class FailingMasterMasterStatement extends MasterMasterStatement {
	
	static double failProbability = 1;
	
	public FailingMasterMasterStatement(Worker w, DBConnection connection)
			throws PESQLException {
		super(w, connection);
	}

	@Override
	protected boolean doExecute(SQLCommand sql, DBResultConsumer resultConsumer) throws PESQLException {
		if (Math.random() < failProbability)
			throw new PECommunicationsException(this.getClass().getSimpleName());
		else
			return super.doExecute(sql, resultConsumer);
	}

	public static void setFailProbability(double failProbability) {
		FailingMasterMasterStatement.failProbability = failProbability;
	}

}
