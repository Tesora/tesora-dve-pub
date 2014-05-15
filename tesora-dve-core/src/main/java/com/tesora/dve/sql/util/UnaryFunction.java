// OS_STATUS: public
package com.tesora.dve.sql.util;

public abstract class UnaryFunction<O, I> {

	public abstract O evaluate(I object);
	
}
