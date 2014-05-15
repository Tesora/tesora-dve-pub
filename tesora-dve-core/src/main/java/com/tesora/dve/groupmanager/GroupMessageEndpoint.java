// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

public class GroupMessageEndpoint implements GroupTopicListener<GroupMessage>, GroupTopicPublisher<GroupMessage> {
	
	private Logger logger = Logger.getLogger(GroupMessageEndpoint.class);

	GroupTopic<GroupMessage> topic;
	
	public GroupMessageEndpoint(String name) {
		this.topic = GroupManager.getCoordinationServices().getTopic(name);
		topic.addMessageListener(this);
	}
	
	public void publish(GroupMessage m) {
		topic.publish(m);
	}

	@Override
	public void onMessage(GroupMessage message) {
		if (logger.isDebugEnabled())
			logger.debug(this.getClass().getSimpleName()+"("+topic+") receives " + message);
        message.execute(Singletons.require(HostService.class));
	}
	
	public void close() {
		topic.removeMessageListener(this);
	}
}
