// OS_STATUS: public
package com.tesora.dve.groupmanager;

public interface GroupTopic<M> extends GroupTopicPublisher<M> {

	void addMessageListener(GroupTopicListener<M> listener);
	
	void removeMessageListener(GroupTopicListener<M> listener);

}
