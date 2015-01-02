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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import com.tesora.dve.locking.impl.CoordinationServices;
import com.tesora.dve.membership.GroupMembershipListener;
import com.tesora.dve.membership.GroupTopic;
import com.tesora.dve.membership.GroupTopicListener;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PELockAbortedException;
import com.tesora.dve.groupmanager.PEReadWriteLock.RWLockMessage.MessageType;

public class PEReadWriteLock implements ReadWriteLock, GroupTopicListener<PEReadWriteLock.RWLockMessage>, GroupMembershipListener {
	
	Logger logger = Logger.getLogger(PEReadWriteLock.class);

	public static class RWLockMessage implements Serializable {
		private static final long serialVersionUID = 1L;
		enum MessageType {
			WRITE_WAITING, WRITE_LOCKED, WRITE_RELEASED,
			ALL_READERS_BLOCKED,
			INIT_REQUEST, INIT_NONWRITER, INIT_WRITER
			}
		InetSocketAddress member;
		MessageType type;
		public RWLockMessage(InetSocketAddress member, MessageType type) {
			this.member = member;
			this.type = type;
		}
	}
	
	class ThreadLocalBoolean extends ThreadLocal<Boolean> {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	}
	
	final String name;
	final CoordinationServices groupServices;
	final GroupTopic<RWLockMessage> topic;
	final InetSocketAddress thisMember;
	final Lock writeLock;
	
	Set<InetSocketAddress> knownMembers = Collections.newSetFromMap(new ConcurrentHashMap<InetSocketAddress, Boolean>());
	Set<InetSocketAddress> membersWaitingForWriteLock = Collections.newSetFromMap(new ConcurrentHashMap<InetSocketAddress, Boolean>());
	Set<InetSocketAddress> outstandingReaders = Collections.newSetFromMap(new ConcurrentHashMap<InetSocketAddress, Boolean>());
	Set<InetSocketAddress> statePendingMembers = Collections.newSetFromMap(new ConcurrentHashMap<InetSocketAddress, Boolean>());
	
	volatile Semaphore readerEntryMutex = new Semaphore(0);
	ReentrantLock readerEntryMutexControl = new ReentrantLock();

	volatile CountDownLatch membersPending = null;
	
	
	AtomicInteger jvmReaderCount = new AtomicInteger();

	AtomicBoolean waitingToSendReadersBlocked = new AtomicBoolean(false);
	
	ThreadLocalBoolean thisThreadHasReadLock = new ThreadLocalBoolean();
	ThreadLocalBoolean thisThreadHasWriteLock = new ThreadLocalBoolean();
	
	public PEReadWriteLock(String name, CoordinationServices groupServices) {
		this.name = name;
		this.groupServices = groupServices;
		this.thisMember = groupServices.getMemberAddress();
		this.topic = groupServices.getTopic(name);
		topic.addMessageListener(this);
		this.writeLock = groupServices.getLock(name);
		statePendingMembers.addAll(groupServices.getMembers());
		
		if (Boolean.getBoolean(getClass().getSimpleName()+".debug"))
			logger.setLevel(Level.DEBUG);
		else
			logger.setLevel(Level.INFO);
		
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" statePendingMembers waiting on " + statePendingMembers.size());
		groupServices.addMembershipListener(this);
		
		topic.publish(new RWLockMessage(thisMember, MessageType.INIT_REQUEST));
	}

    public PEReadWriteLock(String name, InetSocketAddress address, GroupTopic<RWLockMessage> topic,Lock globalLock, Collection<InetSocketAddress> allMembers) {
        this.name = name;
        this.groupServices = null;
        this.thisMember = address;
        this.topic = topic;
        this.topic.addMessageListener(this);
        this.writeLock = globalLock;
        statePendingMembers.addAll(allMembers);

        if (Boolean.getBoolean(getClass().getSimpleName()+".debug"))
            logger.setLevel(Level.DEBUG);
        else
            logger.setLevel(Level.INFO);

        if (logger.isDebugEnabled()) logger.debug(this.toString()+" statePendingMembers waiting on " + statePendingMembers.size());

        topic.publish(new RWLockMessage(thisMember, MessageType.INIT_REQUEST));
    }
	
	@Override
	public Lock readLock() {
		return new Lock() {
			@Override
			public void lock() { getReadLock(); }
			@Override
			public void unlock() { releaseReadLock(); }

			@Override
			public void lockInterruptibly() throws InterruptedException { throw new UnsupportedOperationException(); }
			@Override
			public boolean tryLock() {throw new UnsupportedOperationException(); }
			@Override
			public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {throw new UnsupportedOperationException(); }
			@Override
			public Condition newCondition() {throw new UnsupportedOperationException(); }
		};
	}

	@Override
	public Lock writeLock() {
		return new Lock() {
			@Override
			public void lock() { getWriteLock(); }
			@Override
			public void unlock() { releaseWriteLock(); }

			@Override
			public void lockInterruptibly() throws InterruptedException { throw new UnsupportedOperationException(); }
			@Override
			public boolean tryLock() {throw new UnsupportedOperationException(); }
			@Override
			public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {throw new UnsupportedOperationException(); }
			@Override
			public Condition newCondition() {throw new UnsupportedOperationException(); }
		};
	}

	void getReadLock() {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" getReadLock");
		
		if (thisThreadHasReadLock.get())
			throw new PECodingException(getClass().getSimpleName() + " does not support reentrant locks");
		
		try {
			Semaphore readerMutex = null;
			readerEntryMutexControl.lock();
			try {
				readerMutex = readerEntryMutex;
			} finally { readerEntryMutexControl.unlock(); }
			if (readerMutex != null)
				readerMutex.acquire();
			try {
				jvmReaderCount.incrementAndGet();
				thisThreadHasReadLock.set(true);
				if (logger.isDebugEnabled()) logger.debug(this.toString()+" getReadLock granted");
			} finally {
				if (readerMutex != null)
					readerMutex.release();
			}
		} catch (InterruptedException e) {
			throw new PELockAbortedException(e);
		}
	}

	void releaseReadLock() {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" releaseReadLock");
		thisThreadHasReadLock.set(false);
		if (jvmReaderCount.decrementAndGet() == 0 
				&& membersWaitingForWriteLock.size() > 0 
				&& waitingToSendReadersBlocked.compareAndSet(true, false))
			topic.publish(new RWLockMessage(thisMember, MessageType.ALL_READERS_BLOCKED));
	}

	synchronized void getWriteLock() {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" getWriteLock");

		if (thisThreadHasWriteLock.get())
			throw new PECodingException(getClass().getSimpleName() + " does not support reentrant locks");

		// lock out readers/writers in this jvm
		try {
			readerEntryMutexControl.lock();
			try {
				if (readerEntryMutex == null)
					readerEntryMutex = new Semaphore(1);
			} finally { readerEntryMutexControl.unlock(); }
			readerEntryMutex.acquire();
			if (logger.isDebugEnabled()) logger.debug(this.toString()+" getWriteLock: writer passes mutex");
		} catch (InterruptedException e1) {
			throw new PELockAbortedException(e1);
		}

		// Determine how many sites need to acknowledge all readers blocked
		synchronized (outstandingReaders) {
			for (InetSocketAddress member : knownMembers)
				outstandingReaders.add(member);
			membersPending = new CountDownLatch(outstandingReaders.size());
		}

		// are we promoting a readlock to a writelock?
		if (thisThreadHasReadLock.get())
			jvmReaderCount.decrementAndGet();

		topic.publish(new RWLockMessage(thisMember, MessageType.WRITE_WAITING));
		writeLock.lock();
		try {
			// wait until all sites acknowledge that all of their running readers have completed
			if (logger.isDebugEnabled()) logger.debug(this.toString()+" getWriteLock waiting on " + membersPending.getCount() + " readers");
			membersPending.await();
			if (logger.isDebugEnabled()) logger.debug(this.toString()+" getWriteLock: writer has lock");
			thisThreadHasWriteLock.set(true);
			topic.publish(new RWLockMessage(thisMember, MessageType.WRITE_LOCKED));
		} catch (InterruptedException e) {
			readerEntryMutex.release();
			if (thisThreadHasReadLock.get())
				jvmReaderCount.incrementAndGet();
			throw new PELockAbortedException(e);
		} finally {
			writeLock.unlock();
		}
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" getWriteLock granted");
	}
	
	synchronized void releaseWriteLock() {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" releaseWriteLock");
		if (thisThreadHasReadLock.get())
			jvmReaderCount.incrementAndGet();
		thisThreadHasWriteLock.set(false);
		topic.publish(new RWLockMessage(thisMember, MessageType.WRITE_RELEASED));
	}
	
	@Override
	public void onMembershipEvent(MembershipEventType eventType, InetSocketAddress member) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onMembershipEvent");
		if (eventType == MembershipEventType.MEMBER_REMOVED) {
			knownMembers.remove(member);
			if (membersWaitingForWriteLock.contains(member))
				removeMemberWaitingForWriteLock(member);
			if (logger.isDebugEnabled()) logger.debug(this.toString()+" before: outstandingReaders.hasMember = " + outstandingReaders.contains(member));
			synchronized (outstandingReaders) {
				if (outstandingReaders.remove(member)) {
					if (logger.isDebugEnabled()) logger.debug(this.toString()+" before: membersPending.count = " + membersPending.getCount());
					membersPending.countDown();
					if (logger.isDebugEnabled()) logger.debug(this.toString()+" after: membersPending.count = " + membersPending.getCount());
				}
			}
			if (statePendingMembers.size() > 0)
				removeInitStateMember(member);
		}
	}

	void onWriterRequestMessage(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onWriterRequestMessage");
		readerEntryMutexControl.lock();
		try {
			membersWaitingForWriteLock.add(message.member);
			waitingToSendReadersBlocked.set(true);
			if (false == message.member.equals(thisMember)) {
				// if we're requesting the lock, we already have the reader mutex
				readerEntryMutexControl.lock();
				try {
					if (readerEntryMutex == null)
						readerEntryMutex = new Semaphore(0);
					else
						readerEntryMutex.tryAcquire();
				} finally { readerEntryMutexControl.unlock(); }
			}
		} finally {
			readerEntryMutexControl.unlock();
		}
		if (jvmReaderCount.get() == 0 && waitingToSendReadersBlocked.compareAndSet(true, false))
			topic.publish(new RWLockMessage(thisMember, MessageType.ALL_READERS_BLOCKED));
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onWriterRequestMessage done");
	}

	void onWriterReleaseMessage(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onWriterReleaseMessage");
		removeMemberWaitingForWriteLock(message.member);
	}

	void removeMemberWaitingForWriteLock(InetSocketAddress member) {
		membersWaitingForWriteLock.remove(member);
		readerEntryMutexControl.lock();
		try {
			readerEntryMutex.release();
			if (membersWaitingForWriteLock.size() == 0)
				readerEntryMutex = null;
		} finally { readerEntryMutexControl.unlock(); }
	}

	void onWriterLockedMessage(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onWriterLockedMessage");
	}

	void onAllReadersBlockedMessage(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onAllReadersBlockedMessage");
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" before: outstandingReaders.hasMember = " + outstandingReaders.contains(message.member));
		synchronized (outstandingReaders) {
			if (outstandingReaders.remove(message.member)) {
				if (logger.isDebugEnabled()) logger.debug(this.toString()+" before: membersPending.count = " + membersPending.getCount());
				membersPending.countDown();
				if (logger.isDebugEnabled()) logger.debug(this.toString()+" after: membersPending.count = " + membersPending.getCount() + ": " + outstandingReaders.toString());
			}
		}
	}

	@Override
	public void onMessage(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onMessage from " + message.member);
		switch (message.type) {
		case ALL_READERS_BLOCKED:
			onAllReadersBlockedMessage(message);
			break;
		case WRITE_WAITING:
			onWriterRequestMessage(message);
			break;
		case WRITE_LOCKED:
			onWriterLockedMessage(message);
			break;
		case WRITE_RELEASED:
			onWriterReleaseMessage(message);
			break;
		case INIT_REQUEST:
			onInitRequest(message);
			break;
		case INIT_NONWRITER:
			onInitNonWriter(message);
			break;
		case INIT_WRITER:
			onInitWriter(message);
			break;
		default:
			break;
		}
	}
	
	void onInitWriter(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onInitWriter");
		knownMembers.add(message.member);
		if (statePendingMembers.size() > 0) {
			membersWaitingForWriteLock.add(message.member);
			removeInitStateMember(message.member);
		}
	}

	void onInitNonWriter(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onInitNonWriter");
		knownMembers.add(message.member);
		if (statePendingMembers.size() > 0) {
			removeInitStateMember(message.member);
		}
	}

	void removeInitStateMember(InetSocketAddress member) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" removeInitStateMember");
		if (statePendingMembers.remove(member) 
				&& statePendingMembers.size() == 0
				&& membersWaitingForWriteLock.size() == 0) {
			if (logger.isDebugEnabled()) logger.debug(this.toString()+" removeInitStateMember releases mutex");
			readerEntryMutexControl.lock();
			try {
				if (readerEntryMutex != null)
					readerEntryMutex.release();
				if (membersWaitingForWriteLock.isEmpty())
					readerEntryMutex = null;
			} finally {
				readerEntryMutexControl.unlock();
			}
		}
	}

	void onInitRequest(RWLockMessage message) {
		if (logger.isDebugEnabled()) logger.debug(this.toString()+" onInitRequest");
		knownMembers.add(message.member);
		MessageType msgType = RWLockMessage.MessageType.INIT_NONWRITER;
		if (membersWaitingForWriteLock.contains(thisMember))
			msgType = RWLockMessage.MessageType.INIT_WRITER;
		topic.publish(new RWLockMessage(thisMember, msgType));
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + name + ")@" + hashCode();
	}

}
