// OS_STATUS: public
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
import com.tesora.dve.exceptions.PESQLException;


public class MasterMasterStatement extends SingleDirectStatement {

	public MasterMasterStatement(Worker w, DBConnection jdbcConnection)
			throws PESQLException {
		super(w, jdbcConnection);
	}

	@Override
	protected void onCommunicationFailure(Worker worker) throws PESQLException {
		worker.onCommunicationsFailure();
	}
}
