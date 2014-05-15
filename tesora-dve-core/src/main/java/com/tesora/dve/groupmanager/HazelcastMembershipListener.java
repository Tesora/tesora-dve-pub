// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.tesora.dve.groupmanager.GroupMembershipListener.MembershipEventType;

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
