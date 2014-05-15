// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

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