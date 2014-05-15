// OS_STATUS: public
package com.tesora.dve.groupmanager;

import java.net.InetSocketAddress;
import java.util.Set;


public interface MembershipView {
    InetSocketAddress getMyAddress();

    boolean isInQuorum();

    Set<InetSocketAddress> quorumMembers();

    Set<InetSocketAddress> reachableMembers();

    Set<InetSocketAddress> allMembers();

    Set<InetSocketAddress> activeQuorumMembers();

    Set<InetSocketAddress> inactiveQuorumMembers();

    Set<InetSocketAddress> disabledMembers();
}
