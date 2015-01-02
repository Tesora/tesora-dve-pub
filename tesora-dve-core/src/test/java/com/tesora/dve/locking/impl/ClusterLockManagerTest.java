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
import com.tesora.dve.groupmanager.*;
import com.tesora.dve.locking.*;
import com.tesora.dve.lockmanager.LockClient;

import com.tesora.dve.membership.GroupMembershipListener;
import com.tesora.dve.membership.GroupTopic;
import com.tesora.dve.membership.MembershipView;
import com.tesora.dve.membership.MembershipViewSource;
import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;


public class ClusterLockManagerTest {
    @Rule public JUnitRuleMockery jmock = new JUnitRuleMockery();

    @Mock
    MembershipViewSource memberTracker;

    @Mock
    GroupTopic<ClusterLockManager.ClusterLockMessage> topic;

    @Mock
    ClusterLockManager.LockFactoryBinder<ClusterLock> lockFactoryBinder;

    @Mock
    ReadWriteLock clusterMembershipLock;

    @Mock
    ConcurrentReferenceMap<String, ClusterLock> currentLocks;

    @Mock
    AtomicState<ClusterLock> mapStoredState;

    @Mock
    StateFactory<ClusterLock> boundLockFactory;

    @Mock
    Lock readLock;

    @Mock
    Lock writeLock;

    @Mock
    ClusterLock factoryGeneratedState;

    String lockName = "generation-lock";

    ReadWriteLock constructedLock;
    ClusterLockManager mgr;

    @Before
    public void setup() throws Exception {
        mgr = new ClusterLockManager(memberTracker,topic,lockFactoryBinder,clusterMembershipLock,currentLocks);

        jmock.checking( new Expectations() {{
            oneOf(currentLocks).binding(lockName);will(returnValue(mapStoredState));
            oneOf(lockFactoryBinder).createFactory(lockName);will(returnValue(boundLockFactory));
            oneOf(clusterMembershipLock).readLock();will(returnValue(readLock));
        }});

        constructedLock = mgr.getReadWriteLock(lockName);
        assertNotNull(constructedLock);
    }



    @Test
    public void testGenerateLockEngine() throws Exception {
        ClusterLockManager mgr = new ClusterLockManager(memberTracker,topic,lockFactoryBinder,clusterMembershipLock,currentLocks);

        final String lockName = "myLockName";
        jmock.checking( new Expectations() {{
            oneOf(currentLocks).binding(lockName);will(returnValue(mapStoredState));
            oneOf(lockFactoryBinder).createFactory(lockName);will(returnValue(boundLockFactory));
            oneOf(clusterMembershipLock).readLock();will(returnValue(readLock));
        }});

        ClusterLock proxy = mgr.generateLockEngine(lockName);
        assertNotNull(proxy);
    }

    @Test
    public void testEmptyStateGeneratesNewLock() throws Exception {
        final Sequence callSeq = jmock.sequence("call-seq");


        jmock.checking(new Expectations() {{
//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(null));inSequence(callSeq);
            oneOf(boundLockFactory).newInstance();will(returnValue(factoryGeneratedState));inSequence(callSeq);
            oneOf(mapStoredState).compareAndSet(null, factoryGeneratedState);will(returnValue(true));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).sharedLock(with(aNull(LockClient.class)),with(any(String.class)));inSequence(callSeq);
        }});

        constructedLock.readLock().lock();
    }


    @Test
    public void testReadLock() throws Exception {
        final Sequence callSeq = jmock.sequence("call-seq");

        jmock.checking(new Expectations() {{
//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(factoryGeneratedState));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).sharedLock(with(aNull(LockClient.class)),with(any(String.class)));inSequence(callSeq);
        }});

        constructedLock.readLock().lock();

        jmock.checking(new Expectations() {{
//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(factoryGeneratedState));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).sharedUnlock(with(aNull(LockClient.class)),with(any(String.class)));inSequence(callSeq);
        }});
        constructedLock.readLock().unlock();
    }

    @Test
    public void testWriteLock() throws Exception {
        final Sequence callSeq = jmock.sequence("call-seq");

        jmock.checking(new Expectations() {{
//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(factoryGeneratedState));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).exclusiveLock(with(aNull(LockClient.class)),with(any(String.class)));inSequence(callSeq);
        }});

        constructedLock.writeLock().lock();

        jmock.checking(new Expectations() {{
//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(factoryGeneratedState));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).exclusiveUnlock(with(aNull(LockClient.class)),with(any(String.class)));
            inSequence(callSeq);
        }});
        constructedLock.writeLock().unlock();
    }

    @Test
    public void testOnMembershipEvent() throws Exception {
        final Sequence callSeq = jmock.sequence("call-seq");

        final Set<String> existingLocks = new TreeSet<String>();
        existingLocks.add("one");
        existingLocks.add("two");
        final IntentEntry entry = new IntentEntry(InetSocketAddress.createUnresolved("localhost",1111), LockMode.EXCLUSIVE,15L,15L);
        final ClusterLockManager.MultiplexedLockMessage multiplexedMessage = new ClusterLockManager.MultiplexedLockMessage(lockName,entry);
        final MembershipView builtView = new SimpleMembershipView();

        jmock.checking(new Expectations() {{
            atLeast(1).of(memberTracker).getMembershipView();will(returnValue(builtView));//fetch the membership view

            atLeast(1).of(clusterMembershipLock).writeLock();will(returnValue(writeLock));inSequence(callSeq);
            oneOf(writeLock).lock();inSequence(callSeq);//get the exclusive write lock for the membership list.

            oneOf(currentLocks).keySet();will(returnValue(existingLocks)); //get the set of active locks

            oneOf(currentLocks).binding("one");will(returnValue(mapStoredState));inSequence(callSeq);
            oneOf(lockFactoryBinder).createFactory("one");will(returnValue(boundLockFactory));inSequence(callSeq);
            oneOf(clusterMembershipLock).readLock();will(returnValue(readLock));inSequence(callSeq);//generate the first lock proxy

//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(factoryGeneratedState));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).onMembershipChange(builtView);inSequence(callSeq); //invoke the the first proxy

            oneOf(currentLocks).binding("two");will(returnValue(mapStoredState));inSequence(callSeq);
            oneOf(lockFactoryBinder).createFactory("two");will(returnValue(boundLockFactory));inSequence(callSeq);
            oneOf(clusterMembershipLock).readLock();will(returnValue(readLock));inSequence(callSeq); //generate the second proxy

//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(factoryGeneratedState));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).onMembershipChange(builtView);inSequence(callSeq);//invoke the second proxy

            oneOf(writeLock).unlock();inSequence(callSeq);//unlock the exclusive write lock protecting the membership list

        }});


        mgr.onMembershipEvent(GroupMembershipListener.MembershipEventType.MEMBER_ADDED, InetSocketAddress.createUnresolved("localhost", 1111));
    }

    @Test
    public void testOnMessage() throws Exception {
        final Sequence callSeq = jmock.sequence("call-seq");

        final IntentEntry entry = new IntentEntry(InetSocketAddress.createUnresolved("localhost",1111),LockMode.EXCLUSIVE,15L,15L);
        final ClusterLockManager.MultiplexedLockMessage multiplexedMessage = new ClusterLockManager.MultiplexedLockMessage(lockName,entry);

        jmock.checking(new Expectations() {{
            oneOf(currentLocks).binding(lockName);will(returnValue(mapStoredState));inSequence(callSeq);
            oneOf(lockFactoryBinder).createFactory(lockName);will(returnValue(boundLockFactory));inSequence(callSeq);
            oneOf(clusterMembershipLock).readLock();will(returnValue(readLock));inSequence(callSeq);

//            oneOf(readLock).lock();inSequence(callSeq);
            oneOf(mapStoredState).get();will(returnValue(factoryGeneratedState));inSequence(callSeq);
//            oneOf(readLock).unlock();inSequence(callSeq);
            oneOf(factoryGeneratedState).onMessage(entry);inSequence(callSeq);
        }});


        mgr.onMessage(multiplexedMessage);
    }
}
