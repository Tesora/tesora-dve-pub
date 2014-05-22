package com.tesora.dve.concurrent;

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
