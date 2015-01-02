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

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.tesora.dve.membership.GroupTopicListener;

public class LocalTopicTest implements GroupTopicListener<LocalTopicTest.MessageType>{
	
	enum MessageType { MESSAGE }
	
	AtomicInteger msgCount = new AtomicInteger();
	
	LocalTopic<MessageType> topic = new LocalTopic<MessageType>();
	
	@After
	public void tearDown() throws Exception {
		msgCount.set(0);
	}

	@Test
	public void testAddMessageListener() {
		assertEquals(0, msgCount.get());
		topic.publish(MessageType.MESSAGE);
		assertEquals(0, msgCount.get());
		topic.addMessageListener(this);
		topic.publish(MessageType.MESSAGE);
		assertEquals(1, msgCount.get());
		topic.removeMessageListener(this);
	}

	@Test
	public void testRemoveMessageListener() {
		testAddMessageListener();
		int c = msgCount.get();
		topic.publish(MessageType.MESSAGE);
		assertEquals(c, msgCount.get());
	}

	@Test
	public void testPublish() {
		topic.addMessageListener(this);
		topic.publish(MessageType.MESSAGE);
		assertEquals(1, msgCount.get());
		topic.publish(MessageType.MESSAGE);
		assertEquals(2, msgCount.get());
		topic.removeMessageListener(this);
	}
	
	@Test
	public void testMultipleListeners() {
		Set<GroupTopicListener<MessageType>> listenerSet = new HashSet<GroupTopicListener<MessageType>>();
		for (int i = 0; i < 3; ++i)
			listenerSet.add(new GroupTopicListener<LocalTopicTest.MessageType>() {
				@Override
				public void onMessage(MessageType message) {
					msgCount.incrementAndGet();
				}
			});
		for (GroupTopicListener<MessageType> listener : listenerSet)
			topic.addMessageListener(listener);
		topic.publish(MessageType.MESSAGE);
		assertEquals(3, msgCount.get());
		topic.publish(MessageType.MESSAGE);
		assertEquals(6, msgCount.get());
		Iterator<GroupTopicListener<MessageType>> i = listenerSet.iterator();
		topic.removeMessageListener(i.next());
		i.remove();
		topic.publish(MessageType.MESSAGE);
		assertEquals(8, msgCount.get());
		topic.removeMessageListener(i.next());
		i.remove();
		topic.publish(MessageType.MESSAGE);
		assertEquals(9, msgCount.get());
		topic.removeMessageListener(i.next());
		i.remove();
	}

	@Override
	public void onMessage(MessageType message) {
		msgCount.incrementAndGet();
	}

}
