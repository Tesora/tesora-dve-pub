package com.tesora.dve.common;

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
import java.util.List;
import java.util.TimerTask;
import java.util.Timer;

// this is an object pool that is tuned for environments with lots of threads - currently used for the result
// collector pool.  after about 100 concurrent connections, the commons GenericObjectPool causes nontrivial lock
// contention on borrowObject.  this class is meant to rectify that problem.
public class PEObjectPool<T extends PEPoolableObject> {
	
	// we're going to need two locks: one on the buf list, and one on the free list.
	private final Object bufLock = new Object();
	private final Object freeLock = new Object();
	
	private final PEPoolableObjectFactory<T> factory;
	
	private FreeEntry[] freeList;
	// i.e. the index into the freeList of the last inuse item
	private int lastFree;
	
	// the backing objects
	private PEPoolableObject[] bufList;
	// this is the index of the last object
	private int last;
	
	// expiration parameters
	// how long is something idle before we toss it
	private int threshold;
	private int minIdle;
	private Timer timer;
	
	public PEObjectPool(PEPoolableObjectFactory<T> fact, int minIdle, int initBufSize, int checkInterval, int idleThreshold) {
		factory = fact;
		freeList = new FreeEntry[initBufSize];
		for(int i = 0; i < initBufSize; i++)
			freeList[i] = new FreeEntry(-1);
		// nothing is in use initially
		lastFree = -1;
		bufList = new PEPoolableObject[initBufSize];
		last = -1;
		threshold = idleThreshold;
		this.minIdle = minIdle;
		timer = new Timer();
		timer.schedule(new ExpireTask(this), checkInterval, checkInterval);
	}
	
	// used in tests and what not
	@SuppressWarnings("unchecked")
	public void clear() {
		synchronized(freeLock) {
			for(int i = 0; i < freeList.length; i++)
				// this will clear the entry
				freeList[i].use();
			lastFree = -1;
		}
		List<PEPoolableObject> out = new ArrayList<PEPoolableObject>();
		synchronized(bufLock) {
			for(int i = 0; i < bufList.length; i++) {
				if (bufList[i] != null) {
					out.add(bufList[i]);
					bufList[i] = null;
				}
			}
		}
		for(PEPoolableObject so : out) 
			factory.destroy((T) so);
	}
	
	@SuppressWarnings("unchecked")
	private T getNextFree() throws Exception {
		synchronized(freeLock) {
			T obj = null;
			if (lastFree < 0 || lastFree >= freeList.length) return null;
			FreeEntry fet = freeList[lastFree--];
			if (fet.getIndex() == -1)
				throw new Exception("Found unused slot, expected next free at index " + lastFree + ", total freelist length " + freeList.length);
			obj = (T) bufList[fet.use()];
			return obj;
		}
	}
	
	@SuppressWarnings("unchecked")
	private T makeNewObject() throws Exception {
		PEPoolableObject sot = factory.makeNew();
		synchronized(bufLock) {
			int index = ++last;
			if (index >= bufList.length) {
				// nasty case
				index = -1;
				for(int i = 0; i < bufList.length; i++) {
					if (bufList[i] == null) {
						index = i;
						break;
					}
				}
				if (index == -1) {
					// reallocate
					PEPoolableObject[] newBuf = new PEPoolableObject[bufList.length * 2];
					System.arraycopy(bufList, 0, newBuf, 0, bufList.length);
					index = bufList.length;
					bufList = newBuf;
				}
			}
			bufList[index] = sot;
			sot.setPoolIndex(index);
		}
		return (T) sot;
	}
	
	public T allocate() throws Exception {
//		try {
			T candidate = getNextFree();
			if (candidate == null) 
				candidate = makeNewObject();
			return candidate;
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw e;
//		}
	}
	
	public void deallocate(T obj) throws Exception {
//		try {
			factory.passivate(obj);
			if (obj.getPoolIndex() == -1)
				throw new Exception("Attempt to deallocate an unpooled object");
			synchronized(freeLock) {
				if (++lastFree >= freeList.length) {
					lastFree = freeList.length - 1;
					// reallocate free list
					FreeEntry[] nl = new FreeEntry[freeList.length * 2];
					System.arraycopy(freeList, 0, nl, 0, freeList.length);
					// add new entries at the end
					for(int i = freeList.length; i < nl.length; i++) 
						nl[i] = new FreeEntry(-1);
					freeList = nl;
				}
				freeList[lastFree].setIndex(obj.getPoolIndex());
			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw e;
//		}
	}

	@SuppressWarnings("unchecked")
	public void expire() {
		if (lastFree < minIdle) return;
		long now = System.currentTimeMillis();
		// anything whose timestamp is less than this is a candidate
		long cutoff = now - threshold;
		int[] toDestroy = null;
		// this might not always be that efficient - that's ok - we'll get the next tranche later
		synchronized(freeLock) {
			FreeEntry[] a = new FreeEntry[freeList.length];
			FreeEntry[] b = new FreeEntry[freeList.length];
			int ac = -1;
			int bc = -1;
			for(int i = 0; i < freeList.length; i++) {
				if (i > lastFree) {
					if (ac == -1)
						// really quick exit
						return;
					b[++bc] = freeList[i];
				}
				else if ((lastFree - ac) > minIdle && freeList[i].getTimestamp() < cutoff) {
					a[++ac] = freeList[i];
//					System.out.println("To be destroyed: entry " + i + ", index " + freeList[i].getIndex() + ", lastFree " + lastFree);					
				} else {
					b[++bc] = freeList[i];
				}
			}
			if (ac == -1)
				// quick exit - found nothing
				return;	
			// now a has all the entries we are modifying, and b has all the entries we won't modify
			// we're going to move all the b entries to the beginning of the freeList, and all the a
			// entries to the end.  but first, copy out all the a indexes
			toDestroy = new int[ac+1];
			for(int i = 0; i < toDestroy.length; i++) {
				toDestroy[i] = a[i].getIndex();
				a[i].setIndex(-1);
			}
//			int oldLastFree = lastFree;
			int fc = -1;
			lastFree = -1;
			for(int i = 0; i <= bc; i++) { 
				freeList[++fc] = b[i];
				if (b[i].getIndex() != -1)
					lastFree = fc;
			}
			for(int i = 0; i <= ac; i++) {
				freeList[++fc] = a[i];
			}
//			System.out.println("Evicted " + toDestroy.length + " entries.  Old lastFree " + oldLastFree + ", new lastFree " + lastFree + ", freelist length " + freeList.length);
		}
		PEPoolableObject[] destroyObjs = null;
		synchronized(bufLock) {
			destroyObjs = new PEPoolableObject[toDestroy.length];
			for(int i = 0; i < toDestroy.length; i++) {
				int index = toDestroy[i];
				destroyObjs[i] = bufList[index];
				bufList[index] = null;
			}
		}
		for(PEPoolableObject so : destroyObjs) 
			factory.destroy((T) so);
	}
	
	private static class FreeEntry {
		
		// when -1 - not in use
		private int index;
		private long timestamp;
		
		public FreeEntry(int ind) {
			setIndex(ind);
		}
		
		public void setIndex(int ind) {
			index = ind;
			timestamp = System.currentTimeMillis();
		}
		
		public long getTimestamp() {
			return timestamp;
		}
		
		public int getIndex() {
			return index;
		}
		
		public int use() {
			int ret = index;
			index = -1;
			return ret;
		}
		
	}

	private static class ExpireTask extends TimerTask {

		private final PEObjectPool<?> myPool;
		
		public ExpireTask(PEObjectPool<?> sop) {
			myPool = sop;
		}
		
		@Override
		public void run() {
			myPool.expire();
		}
		
	}
	
}
