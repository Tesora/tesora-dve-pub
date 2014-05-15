// OS_STATUS: public
package com.tesora.dve.concurrent;

public interface PEPromise<T> extends PEFuture<T> {

	PEPromise<T> success(T returnValue);
	
	PEPromise<T> failure(Exception t);
	
	boolean trySuccess(T returnValue);
	
	boolean isFulfilled();
}
