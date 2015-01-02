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

import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.tesora.dve.membership.GroupMembershipListener;
import com.tesora.dve.membership.GroupMembershipListener.MembershipEventType;

public class HazelcastMembershipListener implements MembershipListener {
	
	GroupMembershipListener listener;
	
	public HazelcastMembershipListener(GroupMembershipListener listener) {
		super();
		this.listener = listener;
	}

	@Override
	public void memberAdded(MembershipEvent ev) {
		listener.onMembershipEvent(MembershipEventType.MEMBER_ADDED, ev.getMember().getInetSocketAddress());
	}

	@Override
	public void memberRemoved(MembershipEvent ev) {
		listener.onMembershipEvent(MembershipEventType.MEMBER_REMOVED, ev.getMember().getInetSocketAddress());
	}

}
