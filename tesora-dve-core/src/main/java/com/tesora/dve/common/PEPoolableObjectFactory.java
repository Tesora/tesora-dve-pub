// OS_STATUS: public
package com.tesora.dve.common;

public interface PEPoolableObjectFactory<T extends PEPoolableObject> {

	public T makeNew() throws Exception;
	
	public void passivate(T obj) throws Exception;
	
	public void destroy(T obj);
	
}
