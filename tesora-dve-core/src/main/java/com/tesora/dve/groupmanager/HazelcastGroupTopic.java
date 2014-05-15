// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

public class HazelcastGroupTopic<M> implements GroupTopic<M> {
	
	class HazelcastMessageForwarder<T> implements MessageListener<T> {
		
		GroupTopicListener<T> listener;

		public HazelcastMessageForwarder(GroupTopicListener<T> listener) {
			super();
			this.listener = listener;
		}

		@Override
		public void onMessage(Message<T> m) {
			listener.onMessage(m.getMessageObject());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result
					+ ((listener == null) ? 0 : listener.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			@SuppressWarnings("unchecked")
			HazelcastMessageForwarder<T> other = (HazelcastMessageForwarder<T>) obj;
			return listener == other.listener;
		}

		private HazelcastGroupTopic<M> getOuterType() {
			return HazelcastGroupTopic.this;
		}
		
	}
	
	ITopic<M> hTopic;

	public HazelcastGroupTopic(HazelcastInstance hInstance, String name) {
		hTopic = hInstance.getTopic(name);
	}

	@Override
	public void addMessageListener(GroupTopicListener<M> listener) {
		hTopic.addMessageListener(new HazelcastMessageForwarder<M>(listener));
	}

	@Override
	public void removeMessageListener(GroupTopicListener<M> listener) {
		hTopic.removeMessageListener(new HazelcastMessageForwarder<M>(listener));
	}

	@Override
	public void publish(M message) {
		hTopic.publish(message);
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "(" + hTopic.getName() + ")";
	}

}
