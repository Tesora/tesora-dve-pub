package com.tesora.dve.common.catalog;

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


import io.netty.util.concurrent.DefaultThreadFactory;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name = "auto_incr")
@Cacheable(value=false)
public class AutoIncrementTracker implements CatalogEntity {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger.getLogger(AutoIncrementTracker.class);

	private static final ExecutorService cacheUpdateService = Executors.newCachedThreadPool(new DefaultThreadFactory("autoincr"));

	// global config variables
	private static int minBlockSize = 10;
	private static int maxBlockSize = 1000;
	private static int prefetchThreshold = 50;

	private static class IncrCache {

		final int id;
		long nextIncrValue;
		long maxIncrValue;

		// used to throttle update requests
		final AtomicBoolean pendingUpdate = new AtomicBoolean();

		// adaptive scaling
		final AtomicInteger blockedThreads = new AtomicInteger();
		final AtomicLong demand = new AtomicLong();
		int scaleFactor = 1;
		
		// statistics
		long lastBlockSize;
		long largestBlock;
		long discardedIds;
		long blocksFetched;
		double averageBlockSize;

		IncrCache(int id) {
			this.id = id;
		}

		/** Returns next value with 'blockSize' subsequent values allocated to caller, or -1 if the tracker is reset. */
		long getBlock(long blockSize) {
			if (blockSize < 1)
				throw new PECodingException("A blocksize less than 1 is not supported for table with id = " + id);

			demand.addAndGet(blockSize);
			try {
				synchronized (this) {
					while (size() < blockSize) {
						requestUpdate();
						waitForUpdate();
						if (this != AutoIncrementTracker.incrMap.get(id)) { return -1; } // cache reset
					}
					long nextVal = nextIncrValue;
					nextIncrValue += blockSize;

					double percentUsed = (lastBlockSize - size()) / (double) lastBlockSize * 100.0;
					if (percentUsed > prefetchThreshold) {
						requestUpdate();
					}

					return nextVal;
				}
			} finally {
				demand.addAndGet(0 - blockSize);
			}
		}

		/** Trigger at most one cache update. */
		private void requestUpdate() {
			if (pendingUpdate.compareAndSet(false, true)) {
				cacheUpdateService.submit(new UpdateCacheTask(id));
			}
		}

		/** Block until cache is updated. */
		private void waitForUpdate() {
			try {
				blockedThreads.incrementAndGet();
				synchronized (this) {
					this.wait();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				blockedThreads.decrementAndGet();
			}
		}

		/** Notify any threads waiting for an update. */
		synchronized void releaseWaiters() {
			pendingUpdate.set(false);
			this.notifyAll();
		}

		/** Update the cache. Only called from cacheUpdateService thread. */
		synchronized void addBlock(long blockSize, long nextVal, long newMax) {
			try {
				if (nextIncrValue == 0)
					nextIncrValue = nextVal;
				else if (nextVal > maxIncrValue) {
					// gap in sequence; discard remaining cached values
					discardedIds += maxIncrValue - nextIncrValue;
					nextIncrValue = nextVal;
				}
				maxIncrValue = newMax;
				updateStats(blockSize);
			} finally {
				releaseWaiters();
			}
		}

		/** Update cache statistics and adaptive sizing parameters. */
		private synchronized void updateStats(long blockSize) {
			lastBlockSize = blockSize;
			blocksFetched++;

			if (blockSize > largestBlock)
				largestBlock = blockSize;

			if (averageBlockSize == 0) {
				averageBlockSize = blockSize;
			} else {
				averageBlockSize -= averageBlockSize / 50.0;
				averageBlockSize += blockSize / 50.0;
			}

			if (blockedThreads.get() > 0)
				scaleFactor++;
			else if (scaleFactor > 1)
				scaleFactor--;
		}

		synchronized long size() {
			return maxIncrValue > nextIncrValue ? maxIncrValue - nextIncrValue : 0;
		}

		synchronized void dumpStats() {
			dumpStatsLine("Table " + id,
					String.format("unused ids=%d, discarded ids=%d, next=%d, max=%d, updates=%d, average block=%d, largest block=%d",
							size(), discardedIds, nextIncrValue, maxIncrValue, blocksFetched, (long) Math.ceil(averageBlockSize), largestBlock));
		}

	}

	static class UpdateCacheTask implements Runnable {

		private final int id;

		UpdateCacheTask(int id) {
			this.id = id;
		}

		@Override
		public void run() {
			IncrCache cache = AutoIncrementTracker.incrMap.get(id);
			if (cache == null) { return; }

			CatalogDAO c = null;
			synchronized (cache.pendingUpdate) { // see removeValue, reset
				try {
					if (cache != AutoIncrementTracker.incrMap.get(id)) {
						cache.releaseWaiters();
						return;
					}
					c = CatalogDAOFactory.newInstance();
					long nextId, newMax;
					long blockSize = computeBlockSize(cache);
					AutoIncrementTracker ait = c.findByKey(AutoIncrementTracker.class, id);
					c.nonNestedBegin();
					c.refreshForLock(ait);
					nextId = ait.nextId;
					ait.nextId += blockSize;
					newMax = ait.nextId;
					c.commit();
					cache.addBlock(blockSize, nextId, newMax);
				} catch (Throwable thr) {
					logger.error("Failed to update auto-increment cache for table with id: " + id, thr);
					cache.releaseWaiters();
				} finally {
					if (c != null) { c.close(); }
				}
			}
		}

		private long computeBlockSize(IncrCache cache) {
			long size = cache.demand.get() * 2;
			size = (size > minBlockSize ? size : minBlockSize) * cache.scaleFactor;
			return (size < maxBlockSize ? size : maxBlockSize);
		}
	}

	static ConcurrentHashMap</* incr_id */ Integer, IncrCache> incrMap = new ConcurrentHashMap<Integer, IncrCache>();

	@Id
	@GeneratedValue
	@Column(name = "incr_id")
	private int id;

	@ForeignKey(name="fk_autoincr_table")
	@OneToOne(optional=true,fetch=FetchType.LAZY)
	@JoinColumn(name="table_id")
	private UserTable table; // NOPMD by doug on 04/12/12 11:55 AM

	@ForeignKey(name="fk_autoincr_scope")
	@OneToOne(optional=true,fetch=FetchType.LAZY)
	@JoinColumn(name="scope_id")
	private TableVisibility tenantTable;

	@Basic
	private long nextId;

	public AutoIncrementTracker(UserTable ut) {
		this(ut,null);
	}

	public AutoIncrementTracker(TableVisibility tv) {
		this(tv,null);
	}

	private void init(Long offset) {
		nextId = 0;
		if (offset != null)
			nextId += offset.longValue() - 1;
		nextId += 1;
	}

	public AutoIncrementTracker(UserTable ut, Long offset) {
		table = ut;
		tenantTable = null;
		init(offset);
	}

	public AutoIncrementTracker(TableVisibility tv, Long offset) {
		tenantTable = tv;
		table = null;
		init(offset);
	}

	public AutoIncrementTracker() {
	}

	// used in mysqldump
	public long readNextValue(CatalogDAO c) {
		c.refresh(this);
		return nextId;
	}

	// used in truncate
	public void reset(CatalogDAO c) {
		IncrCache incrCache = getIncrCache(id);
		synchronized(incrCache.pendingUpdate) {
			clearCacheEntry();
			c.refreshForLock(this);
			nextId = 1;
		}
	}

	private static IncrCache getIncrCache(int id) {
		IncrCache incrCache = incrMap.get(id);
		if (incrCache == null) {
			IncrCache newCache = new IncrCache(id);
			incrCache = incrMap.putIfAbsent(id, newCache);
			if (incrCache == null) {
				incrCache = newCache;
			}
		}
		return incrCache;
	}

	public long getNextValue(CatalogDAO c) {
		return getIdBlock(c, 1);
	}

	public static long getNextValue(CatalogDAO c, int id) {
		return getIdBlock(c, id, 1);
	}

	public long getIdBlock(CatalogDAO c, long blockSize) {
		return getIdBlock(c, id, blockSize);
	}

	public static long getIdBlock(CatalogDAO c, int id, long blockSize) {
		long nextVal;
		do {
			nextVal = getIncrCache(id).getBlock(blockSize);
		} while (nextVal < 0);
		return nextVal;
	}

	public void removeValue(CatalogDAO c, long value) {
		IncrCache incrCache = getIncrCache(id);
		synchronized (incrCache.pendingUpdate) {
			synchronized (incrCache) {
				if (value >= incrCache.nextIncrValue && value < incrCache.maxIncrValue) {
					incrCache.nextIncrValue = value + 1;
				} else if (value >= nextId) {
					c.nonNestedBegin();
					c.refreshForLock(this);
					if (value >= nextId) {
						nextId = value + 1;
						incrCache.nextIncrValue = nextId;
					}
					c.commit();
				}
			}
		}
	}

	public static void removeValue(CatalogDAO c, int id, long value) {
		IncrCache incrCache = getIncrCache(id);
		synchronized (incrCache.pendingUpdate) {
			synchronized (incrCache) {
				if (value >= incrCache.nextIncrValue && value < incrCache.maxIncrValue) {
					incrCache.nextIncrValue = value + 1;
					return;
				} else if (value < incrCache.nextIncrValue) {
					return;
				}
			}
		}
		AutoIncrementTracker ait = c.findByKey(AutoIncrementTracker.class, id);
		ait.removeValue(c, value);
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		return null;
	}


	@Override
	public void removeFromParent() {
		// TODO Actually implement the removal of this instance from the parent
	}

	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// TODO Return a valid list of dependents
		return Collections.emptyList();
	}

	@Override
	public int getId() {
		return id;
	}

	public static void clearCache() {
		incrMap.clear();
	}

	@Override
	public void onUpdate() {
		// do nothing
	}

	@Override
	public void onDrop() {
		clearCacheEntry();
	}

	private void clearCacheEntry() {
		IncrCache cache = getIncrCache(id);
		synchronized(cache.pendingUpdate) {
			incrMap.remove(id);
			cache.releaseWaiters();
		}
	}

	public static void setMinBlockSize(int newMin) {
		minBlockSize = newMin;
	}

	public static void setMaxBlockSize(int newMax) {
		maxBlockSize = newMax;
	}

	public static void setPrefetchThreshold(int newThreshold) {
		prefetchThreshold = newThreshold;
	}

	public long getCacheSize() {
		return getIncrCache(id).size();
	}

	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MMM-dd HH:mm:ss");

	private static final void dumpStatsLine(String name, String values) {
		System.out.println("... " + name + "\t: " + values);
	}

	public static void dumpStats() {
		System.out.println("\nAutoIncrementTracker Stats (" + DATE_FORMAT.format(new Date()) + ")");
		dumpStatsLine("Config", String.format("minBlockSize=%d, maxBlockSze=%d, prefetchThreshold=%d", minBlockSize, maxBlockSize, prefetchThreshold));
		if (cacheUpdateService instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor threadPool = (ThreadPoolExecutor) cacheUpdateService;
			dumpStatsLine("Thread pool", String.format("current size=%d, largest size=%d", threadPool.getPoolSize(), threadPool.getLargestPoolSize()));
		}
		for (IncrCache cache: incrMap.values()) {
			cache.dumpStats();
		}
		System.out.println();
	}

}
