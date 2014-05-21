package com.tesora.dve.server.connectionmanager;

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

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.tesora.dve.exceptions.PEException;

public class ConnectionSemaphore extends Semaphore {

	private static final long serialVersionUID = 1L;
	private static AtomicInteger minMemRequirement = new AtomicInteger(1000000);

	AtomicInteger maxConnections = new AtomicInteger();
	double maxPercentWaiting = Double.parseDouble(System.getProperty("ConnectionSemaphore.maxPercentWaiting", "10"))/100;

	public ConnectionSemaphore(int permits) {
		super(permits);
		this.maxConnections.set(permits);
	}
	
	public void safeAcquire() throws PEException {
		if (getQueueLength() > Math.ceil(maxPercentWaiting * maxConnections.get()))
			throw new PEException("Too many requests pending (" + maxConnections.get() * maxPercentWaiting + ")");
		
		verifyMinMemory();

		try {
			super.acquire();
		} catch (InterruptedException e) {
			throw new PEException("Request interrupted");
		}
	}

	public void nonBlockingAcquire() throws PEException {
		verifyMinMemory();

		if (false == super.tryAcquire())
			throw new PEException("Too many connections (maximum = " + maxConnections.get() + ")");
	}

	public void adjustMaxConnections(int newMax) {
		int delta = maxConnections.getAndSet(newMax) - newMax;
		if (delta > 0)
			super.reducePermits(delta);
		else if (delta < 0)
			super.release(Math.abs(delta));
	}
	
	public static void adjustMinMemory(int minMem) {
		minMemRequirement.set(minMem);
	}

	public void verifyMinMemory() throws PEException {
		final Runtime rt = Runtime.getRuntime();
		if (rt.maxMemory() - rt.totalMemory() + rt.freeMemory() < minMemRequirement.get())
			throw new PEException("Not enough memory available to start new connection (Max: " + rt.maxMemory() +
					", Allocated: " + rt.totalMemory() + ", Free: " + rt.freeMemory() + ")");
	}
}