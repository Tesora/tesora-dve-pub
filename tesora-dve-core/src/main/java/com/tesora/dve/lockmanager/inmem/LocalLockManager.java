package com.tesora.dve.lockmanager.inmem;

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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.lockmanager.*;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;

public class LocalLockManager implements LockManager {

	private final Object latch = new Object();
	private final Map<LockSpecification,LockQueue> queues = new HashMap<LockSpecification,LockQueue>();
	// for show support
	private final HashSet<ManagedLock> waiting = new HashSet<ManagedLock>();
	
	@Override
	public AcquiredLock acquire(LockClient c, LockSpecification l, LockType type) {
		// TODO Auto-generated method stub
		ManagedLock ml = new ManagedLock(l,Thread.currentThread(),type, c);
		acquireInternal(ml);
		synchronized(ml) {
			while(!ml.acquiredLock()) try {
				ml.wait();
			} catch (InterruptedException ie) {
				// ignore
			}
		}
		return ml;
	}

	@Override
	public void release(AcquiredLock l) {
		releaseInternal((ManagedLock)l);
	}

	private void reportError(String what) {
		throw new IllegalStateException(what);
	}
	
	private void grantLock(String reason, ManagedLock ml, Collection<ManagedLock> ofQueue) {
		// if ml is an exclusive lock, make sure that it is at the head of the queue
		String message =new MultiReaderSingleWriterBehavior().validateGrantLock(ml, ofQueue);
		if (message != null)
			reportError(message);
		ml.setAcquired(reason);
	}
	
	private void acquireInternal(ManagedLock ml) {
		LockSpecification key = ml.getTarget();
        MultiReaderSingleWriterBehavior p = new MultiReaderSingleWriterBehavior();
		synchronized(latch) {
			LockQueue queue = queues.get(key);
			if (queue == null) {
				queue = new LockQueue();
				queues.put(key,queue);
			}
			// check for double queueing
			if (!p.allowsMultiQueueing()) {
				ManagedLock already = queue.getExistingLock(ml.getClient());
				if (already != null)
					reportError("Double queued lock.  Found " + already.toString() + " while enqueueing " + ml.toString());
			}
			queue.enqueue(ml);
			if (queue.getQueue().size() == 1) {
				grantLock("empty queue",ml,queue.getQueue());
			} else {
				if (p.canAcquire(ml, queue.getQueue())) {
					grantLock("allowed upon create",ml, queue.getQueue());
				} else {
					waiting.add(ml);
				}
			}
		}
	}
	
	private void releaseInternal(ManagedLock ml) {
		LockSpecification key = ml.getTarget();
        MultiReaderSingleWriterBehavior p = new MultiReaderSingleWriterBehavior();
		synchronized(latch) {
			LockQueue queue = queues.get(key);
			ManagedLock head = queue.getHead(); 
			ManagedLock eml = queue.dequeue(ml); 
			if (eml == null)
				reportError("No such lock: " + ml.toString());
			if (queue.getQueue().isEmpty())
				queues.remove(key);
			else {
				List<ManagedLock> toGrant = p.release(head, queue.getQueue());
				for(ManagedLock rml : toGrant) {
					waiting.remove(rml);
					grantLock("upon release",rml, queue.getQueue());
				}
			}
		}
	}

	@Override
	public String assertNoLocks() {
		final StringBuilder buf = new StringBuilder();
		getState(new LockInfoCollector() {

			@Override
			public void onLock(String lockName, ManagedLock ml) {
				// TODO Auto-generated method stub
				buf.append(lockName).append("; ")
					.append(ml.getClient().getName()).append("; ")
					.append(ml.getType().toString()).append("; ")
					.append(ml.acquiredLock() ? "acquired" : "waiting").append("; ")
					.append(ml.getReason()).append("; ")
					.append(ml.getTarget().getOriginator()).append(PEConstants.LINE_SEPARATOR);
			}
			
		});
		if (buf.length() == 0)
			return null;
		return buf.toString();
	}
	
	protected void getState(LockInfoCollector acc) {
		MultiMap<LockSpecification,ManagedLock> sortedWaiters = new MultiMap<LockSpecification,ManagedLock>();
		synchronized(latch) {
			// rebuild waiters as a map
			for(ManagedLock ml : waiting) {
				sortedWaiters.put(ml.getTarget(), ml);
			}
			for(Map.Entry<LockSpecification, LockQueue> me : queues.entrySet()) {
				String lockName = me.getKey().getName();
				for(ManagedLock ml : me.getValue().getQueue()) {
					acc.onLock(lockName, ml);
					sortedWaiters.remove(me.getKey(),ml);
				}
				Collection<ManagedLock> sub = sortedWaiters.get(me.getKey());
				sortedWaiters.remove(me.getKey());
				if (sub == null || sub.isEmpty()) continue;
				for(ManagedLock ml : sub) {
					acc.onLock(lockName, ml);
				}
			}
			for(LockSpecification ls : sortedWaiters.keySet()) {
				Collection<ManagedLock> sub = sortedWaiters.get(ls);
				if (sub == null || sub.isEmpty()) continue;
				for(ManagedLock ml : sub)
					acc.onLock(ls.getName(),ml);
			}
		}
		
	}
	
	@Override
	public IntermediateResultSet showState() {
		// since this is entirely internal, make up a format
		// lock name, conn, type, state
		// where lock name is the lock specification
		// conn is the name of the connection
		// type is the lock type
		// state is either acquired or waiting
		
		ColumnSet cs = new ColumnSet();
		cs.addColumn("lock_name", 255, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("connection", 255, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("lock_type", 12, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("state", 12, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("reason", 255, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("originator", 255, "varchar", java.sql.Types.VARCHAR);
		final List<ResultRow> rows = new ArrayList<ResultRow>();
		getState(new LockInfoCollector() {

			@Override
			public void onLock(String lockName, ManagedLock ml) {
				ResultRow rr = new ResultRow();
				rr.addResultColumn(lockName);
				rr.addResultColumn(ml.getClient().getName());
				rr.addResultColumn(ml.getType().toString());
				rr.addResultColumn((ml.acquiredLock() ? "acquired" : "waiting"));
				rr.addResultColumn(ml.getReason());
				rr.addResultColumn(ml.getTarget().getOriginator());
				rows.add(rr);
				
			}
			
		});
		return new IntermediateResultSet(cs, rows);
	}
	
	interface LockInfoCollector {
		
		void onLock(String lockName, ManagedLock ml);
	}

	private static class LockQueue {
		
		private final LinkedList<ManagedLock> queue;
		private final HashMap<LockClient,ManagedLock> clients;
		
		public LockQueue() {
			queue = new LinkedList<ManagedLock>();
			clients = new HashMap<LockClient,ManagedLock>();
		}
		
		public ManagedLock getExistingLock(LockClient lc) {
			return clients.get(lc);
		}
		
		public void enqueue(ManagedLock ml) {
			queue.add(ml);
			clients.put(ml.getClient(), ml);
		}
		
		public Collection<ManagedLock> getQueue() {
			return queue;
		}
		
		public ManagedLock getHead() {
			return queue.get(0);
		}
		
		public ManagedLock dequeue(ManagedLock ml) {
			ManagedLock was = (queue.remove(ml) ? ml : null);
			clients.remove(ml.getClient());
			return was;
		}
	}
}
