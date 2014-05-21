// OS_STATUS: public
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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class PEDefaultPromise<T> implements PEPromise<T> {
	
	private static final int WAIT_TIMEOUT = 60 * 1000;
	
	static Logger logger = Logger.getLogger( PEDefaultPromise.class );

	static AtomicInteger nextId = new AtomicInteger();
	int thisId = nextId.incrementAndGet();
	
	Set<Listener<T>> listeners = null;
	
	T returnValue;
	Exception thrownException;
	boolean waitersNotified = false;

	@Override
	public PEPromise<T> addListener(Listener<T> listener) {
		synchronized (this) {
			if (listeners == null)
				listeners = new HashSet<Listener<T>>();
			listeners.add(listener);
		}
		return this;
	}

	@Override
	public void removeListener(Listener<T> listener) {
		synchronized (this) {
			listeners.remove(listener);			
		}
	}

	@Override
	public T sync() throws Exception {
		T returnValue;
		synchronized (this) {
			if (logger.isDebugEnabled()) logger.debug(this + ".sync()");
			while (!waitersNotified) {
				try {
					this.wait(WAIT_TIMEOUT);
					if (!waitersNotified) {
						if (logger.isDebugEnabled()) {
							logger.debug(String.format("%s.sync() not released after %d ms", this, WAIT_TIMEOUT));
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (Exception t) {
					thrownException = t;
				}
			}
			if (logger.isDebugEnabled()) 
				logger.debug(this + ".sync() released by " + 
						((thrownException == null) ? ("return value " + this.returnValue) : ("exception " + thrownException.getMessage())));
			if (thrownException != null)
				throw thrownException;
			
			returnValue = this.returnValue;
		}
		return returnValue;
	}

	protected void setReturnValue(T returnValue) {
		this.returnValue = returnValue;
	}

	@Override
	public PEDefaultPromise<T> success(T returnValue) {
		synchronized (this) {
			if (logger.isDebugEnabled()) logger.debug(this + ".success("+returnValue+")");
//			if (executionComplete)
//				throw new IllegalStateException(this.getClass().getSimpleName() + ".success() called multiple times");

			setReturnValue(returnValue);
			
			if (!waitersNotified) {
				try {
					if (listeners != null)
						for (Listener<T> listener : listeners)
							listener.onSuccess(returnValue);
				} finally {
					releaseWaiters();
				}
			}
		}
		return this;
	}

	@Override
	public boolean trySuccess(T returnValue) {
		boolean valueWasSet = false;
		synchronized (this) {
			if (!isFulfilled()) {
				success(returnValue);
				valueWasSet = true;
			}
		}
		return valueWasSet;
	}

	@Override
	public PEDefaultPromise<T> failure(Exception t) {
		synchronized (this) {
			if (logger.isDebugEnabled()) logger.debug(this + ".failure("+t.getMessage()+")", t);
			if (!waitersNotified) {
//				throw new IllegalStateException(this.getClass().getSimpleName() + ".failure() called multiple times (prev exception " + thrownException + ")", t);

				this.thrownException = t;

				try {
					if (listeners != null)
						for (Listener<T> listener : listeners)
							listener.onFailure(t);
				} finally {
					releaseWaiters();
				}
			}
		}
		return this;
	}

	@Override
	public boolean isFulfilled() {
		return returnValue != null || thrownException != null;
	}

	private void releaseWaiters() {
		waitersNotified = true;
		this.notifyAll();
	}

	@Override
	public String toString() {
		return "PEDefaultPromise{"+thisId+"}";
	}

}
