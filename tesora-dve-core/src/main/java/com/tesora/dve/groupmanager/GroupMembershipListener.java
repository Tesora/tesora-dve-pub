// OS_STATUS: public
package com.tesora.dve.groupmanager;

import java.net.InetSocketAddress;

public interface GroupMembershipListener {
	
	enum MembershipEventType { MEMBER_ADDED, MEMBER_ACTIVE, MEMBER_REMOVED }
	
	void onMembershipEvent(MembershipEventType eventType, InetSocketAddress inetSocketAddress);

}
