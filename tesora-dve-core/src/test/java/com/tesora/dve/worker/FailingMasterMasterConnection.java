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

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.worker.MasterMasterConnection;
import com.tesora.dve.worker.SingleDirectStatement;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public class FailingMasterMasterConnection extends MasterMasterConnection {

	public FailingMasterMasterConnection(UserAuthentication auth, StorageSite site) {
		super(auth, site);
	}

	@Override
	protected SingleDirectStatement getNewStatement(Worker w) throws PESQLException {
		return new FailingMasterMasterStatement(w, getConnection());
	}
}
