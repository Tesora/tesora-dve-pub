// OS_STATUS: public
package com.tesora.dve.groupmanager;

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
