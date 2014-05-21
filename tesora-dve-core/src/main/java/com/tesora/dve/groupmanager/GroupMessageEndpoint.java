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
