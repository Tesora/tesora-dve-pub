package com.tesora.dve.server.messaging;

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

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;

public class ConditionalWorkerRequest extends WorkerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final WorkerRequest target;
	private final GuardFunction guard;
	
	public ConditionalWorkerRequest(WorkerRequest targ, GuardFunction guard) {
		super(targ.getContext());
		this.target = targ;
		this.guard = guard;
	}
	
	@Override
	public void executeRequest(Worker w, DBResultConsumer resultConsumer, CompletionHandle<Boolean> promise) {
		if (guard.proceed(w,resultConsumer))
			target.executeRequest(w, resultConsumer, promise);
        else
            promise.success(true);
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return target.getStatisticsNotice();
	}

	@Override
	public MessageType getMessageType() {
		return target.getMessageType();
	}

	@Override
	public MessageVersion getVersion() {
		return target.getVersion();
	}

	public interface GuardFunction {
		
		public boolean proceed(Worker w, DBResultConsumer consumer);
		
	}
	
}
