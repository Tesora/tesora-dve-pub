// OS_STATUS: public
package com.tesora.dve.sql.util;

public class IsInstance<BaseClass> extends UnaryPredicate<BaseClass> {
	
	private final Class<?> match;

	public IsInstance(Class<?> m) {
		match = m;
	}
	
	@Override
	public boolean test(BaseClass object) {
		if (object == null)
			return false;
		return match.isAssignableFrom(object.getClass());
	}


}
