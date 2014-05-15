// OS_STATUS: public
package com.tesora.dve.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Extends ExecutorService to support named tasks. */
public interface NamedTaskExecutorService extends ExecutorService {

	void execute(String name, Runnable command);

	Future<?> submit(String name, Runnable task);

	<T> Future<T> submit(String name, Runnable task, T result);

	<T> Future<T> submit(String name, Callable<T> task);

}
