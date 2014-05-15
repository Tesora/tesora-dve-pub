// OS_STATUS: public
package com.tesora.dve.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public class PECountdownPromise<T> extends PEDefaultPromise<T> {
	
	final AtomicInteger countdown;
	
	public PECountdownPromise(int count) {
		countdown = new AtomicInteger(count);
	}

	@Override
	public PECountdownPromise<T> success(T returnValue) {
		if (countdown.decrementAndGet() == 0)
			super.success(returnValue);
		return this;
	}

	
}
