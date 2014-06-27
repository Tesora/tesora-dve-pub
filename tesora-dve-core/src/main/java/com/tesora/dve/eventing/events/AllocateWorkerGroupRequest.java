package com.tesora.dve.eventing.events;

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


import com.tesora.dve.eventing.EventHandler;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.WorkerGroup.Manager;

public class AllocateWorkerGroupRequest extends Request {

	private Manager manager;
	private UserAuthentication userAuthentication;
	
	public AllocateWorkerGroupRequest(EventHandler requestor,
			Manager manager, UserAuthentication userAuth) {
		super(requestor);
		this.manager = manager;
		this.userAuthentication = userAuth;
	}

	public Manager getManager() {
		return manager;
	}
	
	public UserAuthentication getUserAuthentication() {
		return userAuthentication;
	}
	
}
