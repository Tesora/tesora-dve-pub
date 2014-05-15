// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.google.common.collect.Sets;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.ServerRegistration;

import java.net.InetSocketAddress;
import java.util.*;

public class SimpleMembershipView implements MembershipView {
    InetSocketAddress myAddress;

    Set<InetSocketAddress> catalogRegistered;
    Set<InetSocketAddress> clusterReachable;

    boolean isInQuorum;

    public SimpleMembershipView() {
        this.myAddress = null;
        this.catalogRegistered = Collections.unmodifiableSet(new HashSet<InetSocketAddress>());
        this.clusterReachable = Collections.unmodifiableSet(new HashSet<InetSocketAddress>());
        this.isInQuorum = false;
    }

    public SimpleMembershipView(InetSocketAddress myAddress, Collection<InetSocketAddress> catalogRegistered, Collection<InetSocketAddress> clusterReachable) {
        this.myAddress = myAddress;
        this.catalogRegistered = Collections.unmodifiableSet(new HashSet<InetSocketAddress>(catalogRegistered));
        this.clusterReachable = Collections.unmodifiableSet(new HashSet<InetSocketAddress>(clusterReachable));
        this.isInQuorum = (activeQuorumMembers().size() > inactiveQuorumMembers().size());//quorum only determined by catalog registered members.
    }

    @Override
    public InetSocketAddress getMyAddress(){
        return myAddress;
    }

    @Override
    public boolean isInQuorum(){
        return isInQuorum;
    }

    @Override
    public Set<InetSocketAddress> quorumMembers(){
        return catalogRegistered;
    }

    @Override
    public Set<InetSocketAddress> reachableMembers(){
        return clusterReachable;
    }

    @Override
    public Set<InetSocketAddress> allMembers(){
        return Sets.union(catalogRegistered,clusterReachable);
    }

    @Override
    public Set<InetSocketAddress> activeQuorumMembers(){
        return Sets.intersection(catalogRegistered,clusterReachable);
    }

    @Override
    public Set<InetSocketAddress> inactiveQuorumMembers(){
        return Sets.difference(catalogRegistered, clusterReachable);
    }

    @Override
    public Set<InetSocketAddress> disabledMembers(){
        return Sets.difference(allMembers(), activeQuorumMembers());
    }

    public static SimpleMembershipView disabledView(){
        return new SimpleMembershipView();
    }

    public static MembershipView buildView(InetSocketAddress myAddress, Collection<InetSocketAddress> registered, Collection<InetSocketAddress> reachable){
        return new SimpleMembershipView(myAddress,registered,reachable);
    }

    public static SimpleMembershipView buildView(CatalogDAO catalog, CoordinationServices coord){
        List<ServerRegistration> registeredServers = catalog.findAllRegisteredServers();
        Set<InetSocketAddress> serverIps = new HashSet<InetSocketAddress>();
        for (ServerRegistration serverReg : registeredServers) {
            String addrPort = serverReg.getIpAddress();
            String[] split = addrPort.split(":");
            if (split.length != 2){
//                logger.warn("expected host:port in server registration, "+ addrPort);
                continue;
            }
            InetSocketAddress address = new InetSocketAddress(split[0],Integer.parseInt(split[1]));
            serverIps.add(address);
        }

        return new SimpleMembershipView(coord.getMemberAddress(),serverIps, coord.getMembers());
    }

    public String toString(){
        return String.format("[myAddress=%s, inQuorum=%s, catalog=%s, reachable=%s]",myAddress,isInQuorum,catalogRegistered,clusterReachable);
    }

}
