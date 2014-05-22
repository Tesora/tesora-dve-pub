package com.tesora.dve.groupmanager;

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

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalTopic<T> implements GroupTopic<T> {
	static Logger log = Logger.getLogger(LocalTopic.class);
	static Executor threadPool = Executors.newCachedThreadPool(new DefaultThreadFactory("localTopic"));
	
	Map<GroupTopicListener<T>, AtomicInteger> listeners = new HashMap<GroupTopicListener<T>, AtomicInteger>();
	
	@Override
	public void addMessageListener(GroupTopicListener<T> listener) {
		synchronized (listeners) {
			if (false == listeners.containsKey(listener))
				listeners.put(listener, new AtomicInteger());
			listeners.get(listener).incrementAndGet();
		}
	}

	@Override
	public void removeMessageListener(GroupTopicListener<T> listener) {
		synchronized (listeners) {
			if (listeners.get(listener).decrementAndGet() == 0)
				listeners.remove(listener);
		}
	}

	@Override
	public void publish(final T message) {
		Set<GroupTopicListener<T>> messageReceivers;
		synchronized (listeners) {
			messageReceivers = new HashSet<GroupTopicListener<T>>(listeners.keySet());
		}
		final CountDownLatch latch = new CountDownLatch(messageReceivers.size());
		for (final GroupTopicListener<T> l : messageReceivers)
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					l.onMessage(message);
					latch.countDown();
				}
			});
		try {
			latch.await();
		} catch (InterruptedException e) {
			log.warn(e);
		}
	}

}
