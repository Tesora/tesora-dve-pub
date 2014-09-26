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

import java.sql.ResultSet;

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.GroupDispatch;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.messaging.SQLCommand;


public interface WorkerStatement {

	void execute(int connectionId, SQLCommand sql, GroupDispatch resultConsumer, CompletionHandle<Boolean> promise);
	
	void cancel() throws PESQLException;

	void close() throws PESQLException;

}
