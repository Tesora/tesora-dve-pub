package com.tesora.dve.concurrent;

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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.tesora.dve.clock.NoopTimingService;
import com.tesora.dve.clock.Timer;
import com.tesora.dve.clock.TimingService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.tesora.dve.common.PEContext;
import com.tesora.dve.common.PEThreadContext;

public class PEThreadPoolExecutor extends ThreadPoolExecutor implements NamedTaskExecutorService {

    // minimum log level required to rename threads
	private static final Priority RENAME_THREAD_THRESHOLD = Level.DEBUG;

	private static final Logger logger = Logger.getLogger(PEThreadPoolExecutor.class);

    final TimingService timingService = Singletons.require(TimingService.class, NoopTimingService.SERVICE);

    public PEThreadPoolExecutor(String name) {
		this(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
				new PEDefaultThreadFactory(name));
	}

	public PEThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
	}

	@Override
	public void execute(final Runnable command) {
		super.execute(wrapCommand(command));
	}

	@Override
	public void execute(String name, final Runnable command) {
		super.execute(wrapCommand(command, name));
	}

	@Override
	public <T> Future<T> submit(String name, Callable<T> task) {
		if (task == null)
			throw new NullPointerException();
		RunnableFuture<T> ftask = newTaskFor(task);
		execute(name, ftask);
		return ftask;
	}

	@Override
	public <T> Future<T> submit(String name, Runnable task, T result) {
		if (task == null)
			throw new NullPointerException();
		RunnableFuture<T> ftask = newTaskFor(task, result);
		execute(name, ftask);
		return ftask;
	}

	@Override
	public Future<?> submit(String name, Runnable task) {
		if (task == null)
			throw new NullPointerException();
		RunnableFuture<Void> ftask = newTaskFor(task, null);
		execute(name, ftask);
		return ftask;
	}

	private Runnable wrapCommand(final Runnable command) {
		return wrapCommand(command, null);
	}

	private Runnable wrapCommand(final Runnable command, final String taskName) {
		final PEContext context = PEThreadContext.copy();
        final Timer contextTimer = timingService.getTimerOnThread();
		return new Runnable() {

			@Override
			public void run() {
				PEThreadContext.inherit(context);
                timingService.attachTimerOnThread(contextTimer);
				final String originalName = Thread.currentThread().getName();
				boolean nameChanged = renameThreadForTask(taskName);
				try {
					command.run();
				} finally {
					if (nameChanged) {
						Thread.currentThread().setName(originalName);
					}
                    timingService.detachTimerOnThread();
					PEThreadContext.clear();
				}
			}

		};

	}

	protected boolean renameThreadForTask(String task) {
		if (task == null)
			return false;
		if (!logger.isEnabledFor(RENAME_THREAD_THRESHOLD))
			return false;
		
		Thread th = Thread.currentThread();
		String originalName = th.getName();
		if (originalName.equals(task))
			return false;
		th.setName(originalName + ": " + task);
		return true;
	}

}
