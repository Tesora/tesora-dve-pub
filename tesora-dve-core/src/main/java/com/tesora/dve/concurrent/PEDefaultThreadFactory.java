// OS_STATUS: public
package com.tesora.dve.concurrent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class PEDefaultThreadFactory implements ThreadFactory {

	private static final ConcurrentHashMap<String, AtomicInteger> pools = new ConcurrentHashMap<String, AtomicInteger>();

	private final String prefix;
	private final AtomicInteger threadNumber = new AtomicInteger(1);

	public PEDefaultThreadFactory() {
		this("pool");
	}

	public PEDefaultThreadFactory(final String name) {
		AtomicInteger poolCounter = pools.putIfAbsent(name, new AtomicInteger(2));
		if (poolCounter == null)
			this.prefix = name + "-";
		else
			this.prefix = name + "-" + poolCounter.getAndIncrement() + "-";
	}

	public Thread newThread(final Runnable command) {
		Thread t = new Thread(command, prefix + threadNumber.getAndIncrement());

		if (t.isDaemon())
			t.setDaemon(false);
		if (t.getPriority() != Thread.NORM_PRIORITY)
			t.setPriority(Thread.NORM_PRIORITY);

		return t;
	}

}
