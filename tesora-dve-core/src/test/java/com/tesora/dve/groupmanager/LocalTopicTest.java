// OS_STATUS: public
package com.tesora.dve.groupmanager;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.tesora.dve.groupmanager.GroupTopicListener;
import com.tesora.dve.groupmanager.LocalTopic;

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
