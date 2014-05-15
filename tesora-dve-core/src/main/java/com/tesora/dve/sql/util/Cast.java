// OS_STATUS: public
package com.tesora.dve.sql.util;

public class Cast<O, I> extends UnaryFunction<O, I> {

	@SuppressWarnings("unchecked")
	@Override
	public O evaluate(I object) {
		return (O)object;
	}

}
