package com.tesora.dve.locking.impl;

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

import com.tesora.dve.cas.AtomicState;
import com.tesora.dve.cas.ConcurrentReferenceMap;
import com.tesora.dve.cas.StateFactory;
import com.tesora.dve.cas.impl.ConcurrentReferenceMapImpl;
import com.tesora.dve.cas.impl.LazyState;
import com.tesora.dve.cas.impl.StateEngine;
import com.tesora.dve.clock.MonotonicLongClock;
import com.tesora.dve.clock.WalltimeNanos;
import com.tesora.dve.debug.StringDebugger;
import com.tesora.dve.locking.*;
import com.tesora.dve.lockmanager.*;
import com.tesora.dve.lockmanager.inmem.ManagedLock;
import com.tesora.dve.membership.*;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetSocketAddress;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class ClusterLockManager implements GroupTopicListener<ClusterLockManager.ClusterLockMessage>, GroupMembershipListener, LockManager {
    static final Logger log = Logger.getLogger(ClusterLockManager.class);
    static final String MULTIPLEX_TOPIC_NAME = "pe.topic.locking";

    MembershipViewSource memberTracker;
    GroupTopic<ClusterLockMessage> sharedLockTopic;
    LockFactoryBinder<ClusterLock> lockFactoryBinder;
    ReadWriteLock clusterMembershipLock;
    ConcurrentReferenceMap<String, ClusterLock> currentLocks;
    AtomicBoolean isShutdown = new AtomicBoolean(false);

    public ClusterLockManager(CoordinationServices memberTracker) {
        this.memberTracker = memberTracker;
        this.clusterMembershipLock = new ReentrantReadWriteLock();
        this.currentLocks = new ConcurrentReferenceMapImpl<String, ClusterLock>();

        this.sharedLockTopic = memberTracker.getTopic(MULTIPLEX_TOPIC_NAME);
        this.lockFactoryBinder = new FactoryBinder(new WalltimeNanos(),sharedLockTopic,memberTracker);

        this.sharedLockTopic.addMessageListener(this);
        memberTracker.addMembershipListener(this);
        this.onStart();
    }

    public ClusterLockManager(MonotonicLongClock clock, MembershipViewSource memberTracker, GroupTopic<ClusterLockMessage> topic) {
        this.memberTracker = memberTracker;
        this.sharedLockTopic = topic;
        this.lockFactoryBinder = new FactoryBinder(clock,topic,memberTracker);
        this.clusterMembershipLock = new ReentrantReadWriteLock();
        this.currentLocks = new ConcurrentReferenceMapImpl<String, ClusterLock>();
    }

    ClusterLockManager(MembershipViewSource memberTracker, GroupTopic<ClusterLockMessage> topic, LockFactoryBinder<ClusterLock> lockFactoryBinder, ReadWriteLock clusterMembershipLock, ConcurrentReferenceMap<String, ClusterLock> currentLocks) {
        this.memberTracker = memberTracker;
        this.sharedLockTopic = topic;
        this.lockFactoryBinder = lockFactoryBinder;
        this.clusterMembershipLock = clusterMembershipLock;
        this.currentLocks = currentLocks;
    }

    /**
     * Called when the cluster manager has been properly registered as a topic and membership listener, and will receive
     * all future messages about cluster state.  Typically called inside the coordination service constructor, but needs to be called
     * externally if one of the constructors that does not register listeners is used.
     */
    public void onStart(){
        //This would be so much easier if Hazelcast topics had some concept of topic membership (IE, jBoss service views). 
        this.sharedLockTopic.publish( new LockInitMessage( memberTracker.getMembershipView().getMyAddress() ) );
    }


    public ClusterLock getClusterLock(String lockName){
        return generateLockEngine(lockName);
    }

    public ReadWriteLock getReadWriteLock(final String lockName){
        return new ReadWriteLockAdapter(getClusterLock(lockName));
    }

    ClusterLock generateLockEngine(String lockName) {
        AtomicState<ClusterLock> stateStorage = currentLocks.binding( lockName );
        StateFactory<ClusterLock> lockFactory = lockFactoryBinder.createFactory(lockName);
        Lock membershipSharedLock = clusterMembershipLock.readLock();
        LazyLockState lazyState = new LazyLockState(lockFactory,stateStorage,membershipSharedLock);
        StateEngine<ClusterLock> engine = new StateEngine<ClusterLock>(
                lockName,
                ClusterLock.class ,
                lazyState
        );
        return engine.getProxy(ClusterLock.class);
    }

    @Override
    public void onMembershipEvent(MembershipEventType eventType, InetSocketAddress inetSocketAddress) {
        clusterMembershipLock.writeLock().lock();//make sure no one else can change list of active locks.
        try{
        	if (memberTracker instanceof GroupMembershipListener){
        		GroupMembershipListener gml = (GroupMembershipListener)memberTracker;
        		gml.onMembershipEvent(eventType, inetSocketAddress);//force delivery on coordination service first.
        	}
            //copy the lock names, since the membership invocation might (in theory) trigger the removal of a map entry.
            Set<String> keySet = currentLocks.keySet();
            for (String namedLock : keySet){
                ClusterLock oneShot = generateLockEngine(namedLock);
                oneShot.onMembershipChange(memberTracker.getMembershipView());
            }
        } finally {
            clusterMembershipLock.writeLock().unlock();
        }
    }

    @Override
    public void onMessage(ClusterLockMessage message) {
        if (message instanceof MultiplexedLockMessage){
            MultiplexedLockMessage multi = (MultiplexedLockMessage)message;
            ClusterLock oneShot = generateLockEngine(multi.lockName);
            oneShot.onMessage(multi.entry);
        } else if (message instanceof LockInitMessage){
            LockInitMessage initMessage = (LockInitMessage)message;
            onMembershipEvent(MembershipEventType.MEMBER_ACTIVE, initMessage.address);
        } else {
            log.warn("Ignoring unknown message type, "+message.getClass().getName());
        }
    }

    @Override
    public AcquiredLock acquire(LockClient client, LockSpecification spec, LockType type) {
        ClusterLock rwLock = generateLockEngine(spec.getName());
        if (type == LockType.EXCLUSIVE)
            rwLock.exclusiveLock(client, spec.getOriginator());
        else
            rwLock.sharedLock(client,spec.getOriginator());

        return new ManagedLock(spec,Thread.currentThread(),type, client);
    }

    @Override
    public void release(AcquiredLock al) {
        ClusterLock rwLock = generateLockEngine(al.getTarget().getName());
        if (al.getType() == LockType.EXCLUSIVE)
            rwLock.exclusiveUnlock(al.getClient(), al.getTarget().getOriginator());
        else
            rwLock.sharedUnlock(al.getClient(),al.getTarget().getOriginator());
    }

    @Override
    public String assertNoLocks() {
        clusterMembershipLock.writeLock().lock();//make sure no one else can change list of active locks.
        try{
            Set<String> keySet = currentLocks.keySet();

            boolean added = false;
            StringDebugger debug = new StringDebugger();
            for (String namedLock : keySet){
                ClusterLock oneShot = generateLockEngine(namedLock);
                if ( oneShot.isUnused() )
                    continue;
                added = true;
                debug.entry(namedLock,oneShot);
            }
            if (added)
                return debug.toString();
            else
                return null;
        } finally {
            clusterMembershipLock.writeLock().unlock();
        }
    }

    @Override
    public IntermediateResultSet showState() {
        ColumnSet cs = new ColumnSet();
        cs.addColumn("lock_name", 255, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("connection", 255, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("state", 12, "varchar", java.sql.Types.VARCHAR);        
        cs.addColumn("globalDeclare", 255, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("globalGrant", 255, "varchar", java.sql.Types.VARCHAR);
        cs.addColumn("shareCount", 12, "int", Types.INTEGER);
        cs.addColumn("exclusiveCount", 12, "int", Types.INTEGER);
        cs.addColumn("originator", 255, "varchar", java.sql.Types.VARCHAR);
        List<ResultRow> rows = new ArrayList<ResultRow>();

        clusterMembershipLock.writeLock().lock();//make sure no one else can change list of active locks.
        try{
            //copy the lock names, since the membership invocation might (in theory) trigger the removal of a map entry.
            TreeSet<String> keySet = new TreeSet<String>(currentLocks.keySet());//sorted by lock name
            for (String namedLock : keySet){
                ClusterLock oneShot = generateLockEngine(namedLock);
                oneShot.addShowRow(rows);
            }
        } finally {
            clusterMembershipLock.writeLock().unlock();
        }

        return new IntermediateResultSet(cs, rows);
    }

    public void shutdown() {
        clusterMembershipLock.writeLock().lock();//make sure no one else can change list of active locks.
        try{
            //TODO: presumably we are shutting down clean and someone will pull us out of the catalog so other VMs are OK.
            currentLocks.clear();
            isShutdown.set(true);
        } finally {
            clusterMembershipLock.writeLock().unlock();
        }
    }


    //--------------------------------------------------------------------------------
    //--------------------------------------------------------------------------------
    //--------------------------------------------------------------------------------


    static class LockStateFactory implements StateFactory<ClusterLock> {
        String lockName;
        MonotonicLongClock clock;
        GroupTopic<ClusterLockMessage> sharedTopic;
        MembershipViewSource membershipGetter;

        LockClient client;
        LockSpecification spec;
        LockType type;
        boolean intxn;

        LockStateFactory(String lockName, MonotonicLongClock clock, GroupTopic<ClusterLockMessage> sharedTopic, MembershipViewSource membershipGetter) {
            this.lockName = lockName;
            this.clock = clock;
            this.sharedTopic = sharedTopic;
            this.membershipGetter = membershipGetter;
        }

        //LockClient c, LockSpecification l, LockType type, boolean intxn


        public ClusterLockImpl newInstance(){
            GlobalLockState global = new GlobalLockStateImpl(clock,new MultiplexedPublisher(lockName, sharedTopic), membershipGetter.getMembershipView());
            return new ClusterLockImpl(lockName,null,global);
        }

    }

    public static interface LockFactoryBinder<S> {
        StateFactory<S> createFactory(String name);
    }

    static class FactoryBinder implements LockFactoryBinder<ClusterLock>{
        MonotonicLongClock clock;
        GroupTopic<ClusterLockMessage> sharedTopic;
        MembershipViewSource membershipGetter;

        FactoryBinder(MonotonicLongClock clock, GroupTopic<ClusterLockMessage> sharedTopic, MembershipViewSource membershipGetter) {
            this.clock = clock;
            this.sharedTopic = sharedTopic;
            this.membershipGetter = membershipGetter;
        }

        @Override
        public StateFactory<ClusterLock> createFactory(String name) {
            return new LockStateFactory(name,clock,sharedTopic,membershipGetter);
        }

    }

    static class LazyLockState extends LazyState<ClusterLock> {
        Lock membershipLock;

        public LazyLockState(StateFactory<ClusterLock> lockFactory, AtomicState<ClusterLock> delegate, Lock membershipLock) {
            super(lockFactory,delegate);
            this.membershipLock = membershipLock;
        }

        @Override
        public ClusterLock get() {
            //TODO: figure out why having this read lock causes a deadlock during shutdown, but not having it is OK.
//            membershipLock.lock();
            try{
                return super.get();
            } finally {
//                membershipLock.unlock();
            }
        }

        @Override
        public boolean compareAndSet(ClusterLock expected, ClusterLock value) {
            membershipLock.lock();
            try{

                if ( value.isUnused() )
                    return super.compareAndSet(expected,value); //TODO: change this back to a removal, after figuring out why we it causes livelock via excessive spinning calls
                else
                    return super.compareAndSet(expected,value);
            } finally {
                membershipLock.unlock();
            }
        }

    }

    static class MultiplexedPublisher implements GroupTopic<IntentEntry> {
        String lockName;
        GroupTopic<ClusterLockMessage> sharedTopic;

        MultiplexedPublisher(String lockName, GroupTopic<ClusterLockMessage> sharedTopic) {
            this.lockName = lockName;
            this.sharedTopic = sharedTopic;
        }

        @Override
        public void addMessageListener(GroupTopicListener<IntentEntry> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeMessageListener(GroupTopicListener<IntentEntry> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void publish(IntentEntry message) {
            sharedTopic.publish( new MultiplexedLockMessage(lockName,message) );
        }

    }

    public static abstract class ClusterLockMessage implements Externalizable {
    }


    public static class MultiplexedLockMessage extends ClusterLockMessage {
        private static final int VERSION_MAGIC = 1000;
        String lockName;
        IntentEntry entry;

        public MultiplexedLockMessage() {//don't delete, used by serialization
        }

        public MultiplexedLockMessage(String lockName, IntentEntry message) {
            this.lockName = lockName;
            this.entry = message;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(VERSION_MAGIC);
            out.writeUTF(this.lockName);
            out.writeUTF(this.entry.member.getHostString());
            out.writeInt(this.entry.member.getPort());
            out.writeLong(this.entry.published);
            out.writeLong(this.entry.lastUnlockedOrAcquired);
            out.writeInt(this.entry.state.ordinal());
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int magic = in.readInt();
            if (magic != VERSION_MAGIC)
                throw new IOException("serialized version is not understood, "+magic);

            this.lockName = in.readUTF();
            String host = in.readUTF();
            int port = in.readInt();
            InetSocketAddress addr = new InetSocketAddress(host,port);
            long pubTime = in.readLong();
            long entryTime = in.readLong();
            LockMode mode = LockMode.forOrdinal( in.readInt() );
            this.entry = new IntentEntry(addr,mode,pubTime,entryTime);
        }
    }

    public static class LockInitMessage extends ClusterLockMessage {
        private static final int VERSION_MAGIC = 2000;
        InetSocketAddress address;

        public LockInitMessage() {//don't delete, used by serialization
        }

        public LockInitMessage(InetSocketAddress address) {
            this.address = address;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(VERSION_MAGIC);
            out.writeUTF(this.address.getHostString());
            out.writeInt(this.address.getPort());
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int magic = in.readInt();
            if (magic != VERSION_MAGIC)
                throw new IOException("serialized version is not understood, "+magic);

            String host = in.readUTF();
            int port = in.readInt();
            this.address = new InetSocketAddress(host,port);
        }
    }


}
