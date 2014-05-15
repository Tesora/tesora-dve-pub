// OS_STATUS: public
package com.tesora.dve.concurrent;


public interface PEFuture<T> {
	
	interface Listener<T> {
		void onSuccess(T returnValue);
		void onFailure(Exception e);
	}
	
	PEFuture<T> addListener(Listener<T> listener);
	void removeListener(Listener<T> listener);
	
	T sync() throws Exception;

}
